var fs = require('fs');
var crc32 = require('buffer-crc32');

var sizes = {
  timestamp: 8,
  keysize: 2,
  valsize: 4,
  crc: 4,
  header: 4 + 8 + 2 + 4, // crc + timestamp + keysize + valsize
  offset: 16,
  totalsize: 4
};

var writeCheck = {
  fresh: 1,
  wrap: 2,
  ok: 3
};

var fsflags = {
  O_RDONLY: 0x0,
  O_CREAT: 0x100,
  O_EXCL: 0x200,
  O_RDWR: 0x02,
  O_SYNC: 0x1000
};

var tombstone = new Buffer('medea_tombstone');

var KeyDirEntry = function() {
  this.fileId = null;
  this.valueSize = null;
  this.valuePosition = null;
  this.timestamp = null;
};

var Lock = function() {
  this.fd = null;
  this.type = null;
  this.filename = null;
};

Lock.prototype.isWriteFile = function() {
  return this.type.toLowerCase() === 'write';
};

var FileInfo = function() {
  this.filename = null;
  this.fd = null;
  this.offset = 0;
  this.hintFd = null;
  this.readOnly = true;
  this.hintCrc = new Buffer(sizes.crc);
  this.timestamp = null;
  this.writeLock = null;
};

var FileStatus = function() {
  this.filename = null;
  this.fragmented = null;
  this.deadBytes = null;
  this.totalBytes = null;
  this.oldestTimestamp = null;
  this.newestTimestamp = null;
};

var Medea = module.exports = function(options) {
  this.active = null;
  this.keydir = {};

  options = options || {};

  this.maxFileSize = options.hasOwnProperty('maxFileSize')
    ? options.maxFileSize : 2147483648; //2GB

  this.mergeWindow = options.hasOwnProperty('mergeWindow')
    ? options.mergeWindow : 'always';
  
  this.fragMergeTrigger = options.hasOwnProperty('fragMergeTrigger')
    ? options.fragMergeTrigger : 60; // fragmentation >= 60%

  this.deadBytesMergeTrigger = options.hasOwnProperty('deadBytesMergeTrigger')
    ? options.deadBytesMergeTrigger : 536870912; // dead bytes > 512MB

  this.fragThreshold = options.hasOwnProperty('fragThreshold')
    ? options.fragThreshold : 40; // fragmentation >= 40%

  this.deadBytesThreshold = options.hasOwnProperty('deadBytesThreshold') 
    ? options.deadBytesThreshold : 134217728; // dead bytes > 128MB

  this.smallFileThreshold = options.hasOwnProperty('smallFileThreshold')
    ? options.smallFileThreshold : 10485760 // file < 10MB

  this.maxFoldAge = options.hasOwnProperty('maxFoldAge') ? options.maxFoldAge : -1;
  this.maxFoldPuts = options.hasOwnProperty('maxFoldPuts') ? options.maxFoldPuts : 0;
  this.expirySecs = options.hasOwnProperty('expirySecs') ? options.expirySecs : -1;
  this.dirname = options.hasOwnProperty('dirname') ? options.dirname: null;
  this.readOnly = false;
  this.bytesToBeWritten = 0;
};

Medea.prototype.open = function(dir, options, cb) {
  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  if (typeof dir === 'function') {
    options = {};
    cb = dir;
    dir = this.dirname || 'medea';
  }

  this.dirname = dir;

  var that = this;
  var scanFiles = function(cb) {
    that._getReadableFiles(function(err, arr) {
      that._scanKeyFiles(arr, function() {
        if (cb) cb();
      });
    });
  };

  var next = function(dir, readOnly, cb) {
    if (!readOnly) {
      scanFiles(function() {
        that._acquire(dir, 'write', function(err, writeLock) {
          that._createFile(function(err, file) {
            that._writeActiveFile(writeLock, file, function() {
              if (cb) cb();
            });
          });
        });
      });
    } else {
      scanFiles(function() {
        cb();
      });
    }
  }

  this._ensureDir(dir, function(err) {
    if (err) {
      if (cb) cb(err);
    } else {
      next(dir, that.readOnly, cb);
    }
  });
};

Medea.prototype._ensureDir = function(dir, cb) {
  fs.stat(dir, function(err, stat) {
    if (!stat) {
      fs.mkdir(dir, function(err) {
        if (err) {
          cb(err);
          return;
        }
        if (cb) cb();
      });
    } else {
      if (cb) cb();
    }
  });
};

Medea.prototype._listDataFiles = function(writeFile, mergeFile, cb) {
  var that = this;
  this._dataFileTstamps(function(err, tstamps) {
    if (err) {
      cb(err);
      return;
    }

    var sorted = tstamps.sort(function(a, b) {
      if (a > b) return -1;
      if (a < b) return 1;
      return 0;
    });

    [writeFile, mergeFile].forEach(function(f) {
      if (f) {
        var n = f.replace('.medea.data', '');
        var index = sorted.indexOf(n);
        if (index !== -1) {
          delete sorted[index];
        }
      }
    });

    var ret = sorted.map(function(t) {
      return that.dirname + '/' + t + '.medea.data';
    });

    cb(null, ret);
  });
};

Medea.prototype._scanKeyFiles = function(arr, cb) {
  if (arr.length === 0) {
    cb();
    return;
  }

  var that = this;

  var hintFiles = arr.map(function(f) {
    return f.replace('.medea.data', '.medea.hint');
  });

  var iterator = function(hintFiles, i, max, cb1) {
    var current = hintFiles[i];

    var hintHeaderSize = sizes.timestamp + sizes.keysize + sizes.totalsize + sizes.offset;

    var stream = fs.createReadStream(current);

    var waiting = new Buffer(0);
    var curlen = 0;
    var lastHeaderBuf;
    var lastKeyLen = -1;
    var state = 'findingHeader';
    
    stream.on('data', function(chunk) {
      curlen = chunk.length;

      while (curlen) {
        if (waiting.length) {
          if (!chunk) {
            chunk = new Buffer(0);
          }
          chunk = Buffer.concat([waiting, chunk]);
          curlen = chunk.length;
          waiting = new Buffer(0);
        }

        if (curlen < hintHeaderSize && curlen > sizes.crc && state === 'findingHeader') {
          waiting = chunk;
          curlen = 0;
          return;
        }


        if (state === 'headerFound' && lastKeyLen > -1 && curlen < lastKeyLen) {
          waiting = chunk;
          curlen = 0;
          return;
        }

        if (curlen >= hintHeaderSize && state === 'findingHeader') {
          var headerBuf = chunk.slice(0, hintHeaderSize);

          var keylen = headerBuf.readUInt16BE(sizes.timestamp);
          lastKeyLen = keylen;
          lastHeaderBuf = headerBuf;

          chunk = chunk.slice(headerBuf.length);
          curlen = chunk.length;

          state = 'headerFound';
        } else if (curlen >= lastKeyLen && state === 'headerFound') {
          var keyBuf = chunk.slice(0, lastKeyLen);

          var key = keyBuf.toString();

          var fileId = Number(current.replace(that.dirname + '/', '').replace('.medea.hint', ''));
          if (!that.keydir[key] || (that.keydir[key] && that.keydir[key].fileId === fileId)) {
            var entry = new KeyDirEntry();
            entry.key = key;
            entry.fileId = fileId;
            entry.timestamp = lastHeaderBuf.readDoubleBE(0);
            entry.valueSize = lastHeaderBuf.readUInt32BE(sizes.timestamp + sizes.keysize) - key.length - sizes.header;
            entry.valuePosition = lastHeaderBuf.readDoubleBE(sizes.timestamp + sizes.keysize + sizes.totalsize) + sizes.header + key.length;
            that.keydir[key] = entry;
          }

          chunk = chunk.slice(lastKeyLen);
          curlen = chunk.length;
          lastKeyLen = -1;
          lastHeaderBuf = null;

          state = 'keyFound';
        } else if (curlen === sizes.crc && state === 'keyFound') {
          chunk = new Buffer(0);
          curlen = 0;
        } else {
          if (state === 'keyFound') {
            state = 'findingHeader';
          }
        }
      }
    });
    
    stream.on('end', function() {
      if (i === max) {
        cb1();
      } else {
        iterator(hintFiles, i+1, max, cb1);
      }
    });
  };

  iterator(hintFiles, 0, hintFiles.length - 1, function(err) {
    cb();
  });
};

Medea.prototype._getReadableFiles = function(cb) {
  var that = this;
  var writingFile = this._readActiveFile('write', function(err, writeFile) {
    var mergingFile = that._readActiveFile('merge', function(err, mergeFile) {
      // TODO: Filter out files marked for deletion by successful merge.
      that._listDataFiles(writeFile, mergeFile, function(err, files) {
        cb(null, files);
      });
    });
  });
};

Medea.prototype._readActiveFile = function(type, cb) {
  var filename = this.dirname + '/medea.' + type + '.lock';
  this._acquireLock(filename, false, function(err, lock) {
    if (err) {
      cb(err);
      return;
    }

    var bufs = [];
    var len = 0;
    var ondata = function(chunk) {
      bufs.push(chunk);
      len += chunk.length;
    };

    var onend = function(lockFd) {
      return function() {
        if (!bufs || !bufs.length) {
          cb(null, '');
          return;
        };
        var all = new Buffer(len);
        var offset = 0;
        bufs.forEach(function(buf) {
          buf.copy(all, 0, offset, buf.length);
          offset += buf.length;
        });

        var text = all.toString();

        var arr = text.replace('\n', '').split(' ');

        // release lock?
        if (arr.length && arr[1]) {
          cb(null, arr[1]);
        } else {
          cb(null, '');
        }
      };
    };

    var stream = fs.createReadStream(filename, { fd: lock.fd });
    stream.on('data', ondata);
    stream.on('end', onend(lock.fd));
    stream.on('error', function(err) {
      cb(err);
    });

    stream.resume();
  });
};

Medea.prototype._writeActiveFile = function(writeLock, file, cb) {
  if (file) {
    this.active = file;
  }

  var filename = file ? file.filename : '';
  var lockfile = this.dirname + '/medea.write.lock';
  var stream = fs.createWriteStream(lockfile, { fd: writeLock.fd, start: 0 });
  stream.write(process.pid + ' ' + (filename || '') + '\n', function(err) {
    if (err) {
      console.log('Error on writing active file to write lock.', err);
    }
    cb(null);
  });
};

Medea.prototype._writeActiveFileSync = function(writeLock, file) {
  if (file) {
    this.active = file;
  }

  var filename = file ? file.filename : '';
  var lockfile = this.dirname + '/medea.write.lock';

  var buf = new Buffer(process.pid + ' ' + (filename || '') + '\n');
  fs.writeSync(writeLock.fd, buf, 0, buf.length, 0);
};

Medea.prototype._acquire = function(dir, type, cb) {
  var filename = dir + '/medea.' + type + '.lock';

  var that = this;
  var writeFile = function() {
    that._acquireLock(filename, true, function(err, writeLock) {
      if (err) {
        console.log('Error on acquiring write lock.', err);
      }
      that._writeActiveFile(writeLock, null, function(err) {
        if (err) {
          console.log('Error on writing active file:', err);
        }
        cb(null, writeLock);
      });
    });
  };

  var bufs = [];
  var len = 0;
  var ondata = function(data) {
    bufs.push(data);
    len += data.length;
  };

  var onend = function(fd) {
    return function() {
      var contents = new Buffer(len);
      var i = 0;
      bufs.forEach(function(buf) {
        buf.copy(contents, i, 0, buf.length);
        i += buf.length;
      });

      var c = contents.toString().split(' ');

      if (c.length && c[0].length) {
        var pid = c[0];
        var fname;
        if (c[1] && c[1].length && c[1] !== '\n') {
          fname = c[1];
        }

        if (pid == process.pid) {
          cb(null, that.writeLock);
        } else {
          fs.unlink(filename, function(err) {
            if (err) {
              cb(err);
              return;
            }
            that._acquire(dir, type, cb);
            return;
          });
        }
      } else {
        fs.close(fd, function(err) {
          if (err) {
          }
          writeFile();
        });
      }
    };
  };

  this._acquireLock(filename, false, function(err, writeLock) {
    if (err) {
      if (err.code === 'ENOENT') {
        // this is okay.  move on.
        writeFile();
        return;
      }
      cb(err);
      return;
    }
    var stream = fs.createReadStream(filename, { fd: writeLock.fd });
    stream.on('data', ondata);
    stream.on('end', onend(writeLock.fd));
    stream.on('error', function(err) {
      cb(err);
    });

    stream.resume();
  });
};

Medea.prototype._acquireLock = function(filename, isWriteLock, cb) {
  var writeLockFlags = fsflags.O_CREAT | fsflags.O_EXCL | fsflags.O_RDWR | fsflags.O_SYNC;

  var flags = isWriteLock ? writeLockFlags : fsflags.O_RDONLY;

  var that = this;

  fs.open(filename, flags, 0600, function(err, fd) {
    if (err) {
      cb(err);
      return;
    }

    var writeLock = new Lock();
    writeLock.filename = filename;
    writeLock.type = 'write';
    writeLock.fd = fd;
    that.writeLock = writeLock;

    cb(null, writeLock);
  });
};

Medea.prototype._open = function(file, cb) {
  fs.open(file.filename, 'a+', function(err, value) {
    if (err) {
      cb(err);
      return;
    }

    file.fd = value;
    cb(null, file);
  });
};

Medea.prototype._openSync = function(file) {
  var fd = fs.openSync(file.filename, 'a+');
  file.fd = fd;
  return file;
};

Medea.prototype._checkWrite = function() {
  var ret = writeCheck.ok;

  if (this.bytesToBeWritten > this.maxFileSize) {
    ret = writeCheck.wrap;
  }

  return ret;
};

Medea.prototype.close = function(cb) {
  var that = this;
  that._closeForWriting(this.active, function() {
    fs.unlink(that.writeLock.filename, function(err) {
      fs.close(that.writeLock.fd, function() {
        if (cb) cb();
      });
    });
  });
};

Medea.prototype._closeForWriting = function(file, cb) {
  if (!file || file.offset === 0 || file.readOnly) {
    if (cb) cb();
    return;
  }

  var that = this;
  if (file.hintFd) {
    this._closeHintFile(file, function() {
      if (cb) cb();
    });
  } else {
    if (cb) cb();
  }
};

Medea.prototype._closeForWritingSync = function(file) {
  if (!file || file.offset === 0 || file.readOnly) {
    return;
  }

  if (file.hintFd) {
    this._closeHintFileSync();
  }
};

Medea.prototype._closeHintFile = function(file, cb) {
  if (!file.hintFd || file.closingHintFile) {
    if (cb) cb();
    return;
  }
  file.closingHintFile = true;
  var hintFilename = this.dirname + '/' + file.timestamp + '.medea.hint';
  
  var crcBuf = new Buffer(sizes.crc);
  file.hintCrc.copy(crcBuf, 0, 0, file.hintCrc.length);

  var that = this;
  this._write(hintFilename, file.hintFd, crcBuf, function() {
    fs.fsync(file.hintFd, function(err) {
      if (err) {
        console.log('Error fsyncing hint file during close.', err);
        if (cb) cb(err);
        return;
      }
      fs.close(file.hintFd, function(err) {
        file.hintFd = null;
        file.hintCrc = new Buffer(sizes.crc);
        if (cb) cb();
      });
    });
  });
};

Medea.prototype._closeHintFileSync = function(file) {
  if (!file.hintFd || file.closingHintFile) {
    if (cb) cb();
    return;
  }
  file.closingHintFile = true;
  var hintFilename = this.dirname + '/' + file.timestamp + '.medea.hint';
  
  var crcBuf = new Buffer(sizes.crc);
  file.hintCrc.copy(crcBuf, 0, 0, file.hintCrc.length);

  var that = this;
  this._writeSync(hintFilename, file.hintFd, crcBuf);
  fs.fsyncSync(file.hintFd);
  fs.closeSync(file.hintFd);
  file.hintFd = null;
  file.hintCrc = new Buffer(sizes.crc);
};

Medea.prototype.put = function(k, v, cb) {
  this._put(k, v, cb);
};

Medea.prototype._createFile = function(cb) {
  var that = this;
  this._mostRecentTstamp(function(err, stamp) {
    stamp = stamp + 1;
    var filename = that.dirname + '/' + stamp + '.medea.data';
    var file = new FileInfo();
    file.filename = filename;
    that._ensureDir(that.dirname, function(err) {
      that._open(file, function(err, val1) {
        if (err) {
          cb(err);
          return;
        }
        var hintFilename = that.dirname + '/' + stamp + '.medea.hint';
        var hintFile = new FileInfo();
        hintFile.filename = hintFilename;
        that._open(hintFile, function(err, val2) {
          if (err) {
            cb(err)
            return;
          }

          file.readOnly = false;
          file.fd = val1.fd;
          file.hintFd = val2.fd;
          file.offset = 0;
          file.timestamp = stamp;

          if (cb) cb(null, file);
        });
      });
    });
  });
};

Medea.prototype._createFileSync = function() {
  var stamp = this._mostRecentTstampSync();
  //var stamp = this.active.stamp;
  stamp = stamp + 1;
  var filename = this.dirname + '/' + stamp + '.medea.data';
  var file = new FileInfo();
  file.filename = filename;
  var val1 = this._openSync(file)

  var hintFilename = this.dirname + '/' + stamp + '.medea.hint';
  var hintFile = new FileInfo();
  hintFile.filename = hintFilename;
  var val2 = this._openSync(hintFile);

  file.readOnly = false;
  file.fd = val1.fd;
  file.hintFd = val2.fd;
  file.offset = 0;
  file.timestamp = stamp;

  return file;
};

Medea.prototype._dataFileTstamps = function(cb) {
  fs.readdir(this.dirname, function(err, files) {
    if (err) {
      cb(err);
      return;
    }

    var tstamps = [];
    
    files.forEach(function(file) {
      var match = file.match(/^([0-9]+).medea.data/);
      if (match && match.length && match[1]) {
        tstamps.push(Number(match[1]));
      }
    }); 

    cb(null, tstamps);
  });
};

Medea.prototype._dataFileTstampsSync = function() {
  var files = fs.readdirSync(this.dirname);
  var tstamps = [];

  files.forEach(function(file) {
    var match = file.match(/^([0-9]+).medea.data/);
    if (match && match.length && match[1]) {
      tstamps.push(Number(match[1]));
    }
  }); 
    
  return tstamps;
};

Medea.prototype._mostRecentTstamp = function(cb) {
  this._dataFileTstamps(function(err, stamps) {
    if (err) {
      cb(err);
      return;
    }

    if (stamps.length) {
      cb(null, stamps.sort(function(a, b) {
        if (a > b) return - 1;
        if (a < b) return 1;
        return 0;
      })[0]);
    } else {
      cb(null, 0);
    }
  });
};

Medea.prototype._mostRecentTstampSync = function() {
  var stamps = this._dataFileTstampsSync();
  if (stamps.length) {
    return stamps.sort(function(a, b) {
      if (a > b) return -1;
      if (a < b) return 1;
      return 0;
    })[0];
  } else {
    return 0;
  }
};

Medea.prototype._put = function(k, v, cb) {
  if (!(k instanceof Buffer)) {
    k = new Buffer(k.toString());
  }

  if (!(v instanceof Buffer)) {
    v = new Buffer(v.toString());
  }

  var bytesToBeWritten = sizes.header + k.length + v.length;
  this.bytesToBeWritten += bytesToBeWritten;
  
  var that = this;
  var next = function(cb) { cb(null, that.active); };
  var check = this._checkWrite();
  if (check === writeCheck.wrap) {
    next = function(cb) {
      var file = that._wrapWriteFileSync();
      cb(null, file);
    };
  }

  var that = this;
  next(function(err, file) {
    var ts = Date.now();


    var crc = new Buffer(sizes.crc);
    var timestamp = new Buffer(sizes.timestamp);
    var keysz = new Buffer(sizes.keysize);
    var valuesz = new Buffer(sizes.valsize);
    var key = new Buffer(k);
    var value = new Buffer(v);

    timestamp.writeDoubleBE(ts, 0);
    keysz.writeUInt16BE(key.length, 0);
    valuesz.writeUInt32BE(value.length, 0);

    var bufs = Buffer.concat([timestamp, keysz, valuesz, key, value]);
    var crcBuf = crc32(bufs);

    crcBuf.copy(crc, 0, 0, crcBuf.length);

    var line = Buffer.concat([crc, bufs]);

    that._write(file.filename, file.fd, line, function() {
      var oldOffset = file.offset;
      file.offset = file.offset + line.length;
      var offsetField = new Buffer(sizes.offset);
      var totalSizeField = new Buffer(sizes.totalsize);

      var totalSz = key.length + value.length + sizes.header;
      offsetField.writeDoubleBE(oldOffset, 0);
      totalSizeField.writeUInt32BE(totalSz, 0);

      var hintBufs = Buffer.concat([timestamp, keysz, totalSizeField, offsetField, key]);

      that._write(that.dirname + '/' + file.timestamp + '.medea.hint', file.hintFd, hintBufs, function() {
        file.hintCrc = crc32(hintBufs, file.hintCrc);

        var entry = new KeyDirEntry();
        entry.fileId = file.timestamp;
        entry.valueSize = value.length;
        entry.valuePosition = oldOffset + sizes.header + key.length;
        entry.timestamp = ts;

        that.keydir[k] = entry;

        if (cb) cb();
      });
    });
  });
};

// This differs in order from Erlang Bitcask.
// This may cause files to go slightly over max file size, 
// but it allows for fast async behavior without blocking.
//
// 1. Create new wrapped file.
// 2. Switch active record to new file.
// 3. Close for writing.
//
// Deprecated for _wrapWriteFileSync
Medea.prototype._wrapWriteFile = function(cb) {
  var oldFile = this.active;

  var that = this;
  that._createFile(function(err, file) {
    if (err) {
      console.log('Error wrapping file.', err);
    }
    that._writeActiveFile(that.writeLock, file, function() {
      that._closeForWriting(oldFile, function() {
        that.bytesToBeWritten = 0;
        if (cb) cb(null, file);
      });
    });
  });
};

Medea.prototype._wrapWriteFileSync = function() {
  var oldFile = this.active;
  this.isWrapping = true;
  var file = this._createFileSync();
  this._writeActiveFileSync(this.writeLock, file);
  this._closeForWritingSync(oldFile);
  this.bytesToBeWritten = 0;
  return file;
};

Medea.prototype._fileTimestamp = function(file) {
  var f = file.replace('\\', '/');
  var lastSlashLocation = file.lastIndexOf('/');
  var dotLocation = file.indexOf('.');
  var ts = file.substring(lastSlashLocation+1, dotLocation);
  return Number(ts);
};

Medea.prototype._write = function(filename, fd, bufs, cb) {
  var stream = fs.createWriteStream(filename, { flags: 'a', fd: fd });
  stream.write(bufs, cb);
};

Medea.prototype._writeSync = function(filename, fd, bufs) {
  return fs.writeSync(fd, bufs, 0, bufs.length, 0);
};

Medea.prototype.get = function(key, cb) {
  var entry = this.keydir[key];
  if (entry) {
    var readBuffer = new Buffer(entry.valueSize);
    var filename = this.dirname + '/' + entry.fileId + '.medea.data';
    var stream = fs.createReadStream(filename, { start: entry.valuePosition, end: entry.valuePosition + entry.valueSize - 1 });

    var bufs = [];
    var len = 0;
    stream.on('data', function(chunk) {
      bufs.push(chunk);
      len += chunk.length;
    });
    
    stream.on('end', function() {
      var val = new Buffer(len);
      var ofs = 0;
      bufs.forEach(function(b) {
        b.copy(val, 0, ofs, b.length);
        ofs += b.length;
      });

      if (val.toString() !== tombstone.toString()) {
        cb(val);
      } else {
        if (cb) cb();
      }

    });
  } else {
    if (cb) cb();
  }
};

Medea.prototype.remove = function(key, cb) {
  var that = this;
  this.put(key, tombstone, function() {
    if (that.keydir[key]) {
      delete that.keydir[key];
    }

    if(cb) cb();
  });
};

Medea.prototype.sync = function(cb) {
  fs.fsync(this.active.fd, cb);
};
