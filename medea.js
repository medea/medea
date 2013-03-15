var fs = require('fs');
var crc32 = require('buffer-crc32');

var sizes = {
  timestamp: 32,
  keysize: 16,
  valsize: 32,
  crc: 32,
  header: 32 + 32 + 16 + 32, // crc + timestamp + keysize + valsize
  offset: 64,
  totalSize: 32
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
  this.dirname = null;
  this.readOnly = true;
};

Medea.prototype.open = function(dir, options, cb) {
  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  if (typeof dir === 'function') {
    options = {};
    cb = dir;
    dir = 'medea';
  }

  this.readOnly = options.hasOwnProperty('readOnly') ? options.readOnly : false;
  this.active = new FileInfo();
  this.active.filename =  dir + '/' + Date.now().toString() + '.medea.data';
  this.active.readOnly = this.readOnly;

  var that = this;

  dir = __dirname + '/' + dir;
  this.dirname = dir;

  var next = function(dir, readOnly, cb) {
    if (!readOnly) {
      that._acquire(dir, 'write', function(err, writeLock) {
        that._createFile(function(err, file) {
          that._writeActiveFile(writeLock, file.filename, function() {
            if (cb) cb();
          });
        });
      });
    } else {
      cb();
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

Medea.prototype._writeActiveFile = function(writeLock, filename, cb) {
  var lockfile = this.dirname + '/medea.write.lock';
  var stream = fs.createWriteStream(lockfile, { fd: writeLock.fd, start: 0 });
  stream.write(process.pid + ' ' + (filename || '') + '\n', function(err) {
    if (err) {
      console.log('Error on writing active file to write lock.', err);
    }
    cb(null);
  });
};

Medea.prototype._acquire = function(dir, type, cb) {
  var filename = dir + '/medea.' + type + '.lock';

  var that = this;
  var writeFile = function() {
    that._acquireLock(filename, true, function(err, writeLock) {
      if (err) {
        console.log('Error on acquiring write lock.', err);
      }
      that._writeActiveFile(writeLock, '', function(err) {
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

Medea.prototype._checkWrite = function(key, value) {
  var size = sizes.header + key.length + value.length;

  var ret = writeCheck.ok;

  if (this.active.offset === 0) {
    ret = writeCheck.fresh;
  } else if (this.active.offset + size > this.maxFileSize) {
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

  if (file.hintFd) {
    this._closeHintFile(file, function() {
      //fs.fsync(file.hintFd, function(err) {
        if (cb) cb();
      //});
    });
  } else {
    if (cb) cb();
  }
};

Medea.prototype._closeHintFile = function(file, cb) {
  if (!file.hintFd || file.closingHintFile) {
    if (cb) cb();
    return;
  }
  file.closingHintFile = true;
  var hintFilename = this.dirname + '/' + file.timestamp + '.medea.hint';
  var keysz = new Buffer(sizes.keysize);
  keysz.writeDoubleBE(0, 0);

  var tstamp = new Buffer(sizes.timestamp);
  tstamp.writeDoubleBE(0, 0);

  var offset = new Buffer(sizes.offset);
  offset.writeDoubleBE(0, 0);

  var totalSize = file.hintCrc;
  
  var key = new Buffer('');

  var bufs = Buffer.concat([tstamp, keysz, totalSize, offset, key]);
  var that = this;
  this._write(hintFilename, file.hintFd, bufs, function() {
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

Medea.prototype.put = function(k, v, cb) {
  var that = this;
  that._put(k, v, this.active.offset, cb);
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
          that.active = new FileInfo();
          that.active.filename = file.filename;
          that.active.readOnly = false;
          that.active.fd = val1.fd;
          that.active.hintFd = val2.fd;
          that.active.offset = 0;
          that.active.timestamp = stamp;

          if (cb) cb(null, that.active);
        });
      });
    });
  });
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

Medea.prototype._put = function(k, v, offset, cb) {
  var next = function(cb) { cb(); };
  var check = this._checkWrite(k, v);
  if (check === writeCheck.wrap) {
    next = this._wrapWriteFile.bind(this);
  }

  var that = this;
  next(function() {
    if (!v instanceof Buffer) {
      v = new Buffer(v);
    }

    var ts = Date.now();

    var crc = new Buffer(sizes.crc);
    var timestamp = new Buffer(sizes.timestamp);
    var keysz = new Buffer(sizes.keysize);
    var valuesz = new Buffer(sizes.valsize);
    var key = new Buffer(k);
    var value = new Buffer(v);

    timestamp.writeDoubleBE(ts, 0);
    keysz.writeDoubleBE(key.length, 0);
    valuesz.writeDoubleBE(value.length, 0);

    var bufs = Buffer.concat([timestamp, keysz, valuesz, key, value]);
    var crcBuf = crc32(bufs);

    crcBuf.copy(crc, 0, 0, crcBuf.length);

    var line = Buffer.concat([crc, bufs]);

    that._write(that.active.filename, that.active.fd, line);

    var offsetField = new Buffer(sizes.offset);
    var totalSizeField = new Buffer(sizes.totalSize);

    var totalSz = key.length + value.length + sizes.header;
    offsetField.writeDoubleBE(offset, 0);
    totalSizeField.writeDoubleBE(totalSz, 0);

    var hintBufs = Buffer.concat([timestamp, keysz, totalSizeField, offsetField, key]);

    that._write(that.dirname + '/' + that.active.timestamp + '.medea.hint', that.active.hintFd, hintBufs);

    that.active.offset = that.active.offset + line.length;
    that.active.hintCrc = crc32(hintBufs, that.active.hintCrc);

    var entry = new KeyDirEntry();
    entry.fileId = that.active.timestamp;
    entry.valueSize = value.length;
    entry.valuePosition = offset + sizes.header + key.length;
    entry.timestamp = ts;

    that.keydir[k] = entry;

    if (cb) cb();
  });
};

// This differs in order from Erlang Bitcask.
// This may cause files to go slightly over max file size, 
// but it allows for fast async behavior without blocking.
//
// 1. Create new wrapped file.
// 2. Switch active record to new file.
// 3. Close for writing.
Medea.prototype._wrapWriteFile = function(cb) {
  var oldFile = this.active;

  var that = this;
  that._createFile(function(err, file) {
    that._writeActiveFile(that.writeLock, file.filename, function() {
      that._closeForWriting(oldFile, function() {
        if (cb) cb();
      });
    });
  });
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

Medea.prototype.get = function(key, cb) {
  var entry = this.keydir[key];
  if (entry) {
    var readBuffer = new Buffer(entry.valueSize);
    fs.read(this.active.fd, readBuffer, 0, entry.valueSize, entry.valuePosition, function(err, bytesRead, buffer) {
      if (buffer !== tombstone) {
        cb(readBuffer);
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
