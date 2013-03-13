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
        if (err) {
          cb(err);
          return;
        }
        cb();
        //that._open(that.active, cb);
      });
    } else {
      //that._open(that.active, cb);
      cb();
    }
  }

  this._ensureDir(dir, function(err) {
    if (err) {
      cb(err);
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
        cb();
      });
    } else {
      cb();
    }
  });
};

Medea.prototype._writeActiveFile = function(writeLock, filename, cb) {
  var lockfile = this.dirname + '/medea.write.lock';
  var stream = fs.createWriteStream(lockfile, { fd: writeLock.fd, start: 0 });
  stream.write(process.pid + ' ' + (filename || '') + '\n');
  this.writeLock = writeLock;
  cb(null);
};

Medea.prototype._acquire = function(dir, type, cb) {
  var filename = dir + '/medea.' + type + '.lock';

  var that = this;
  var writeFile = function() {
    that._acquireLock(filename, true, function(err, writeLock) {
      that._writeActiveFile(writeLock, '', function(err) {
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

        if (pid === process.pid) {
          cb(null, ret);
        } else {
          fs.unlink(filename, function(err) {
            if (err) {
              cb(err);
              return;
            }
            writeFile();
          });
        }
      } else {
        writeFile();
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

    stream.resume();
  });
};

Medea.prototype._acquireLock = function(filename, isWriteLock, cb) {
  var writeLockFlags = fsflags.O_CREAT | fsflags.O_EXCL | fsflags.O_RDWR | fsflags.O_SYNC;

  var flags = isWriteLock ? writeLockFlags : fsflags.O_RDONLY;

  fs.open(filename, flags, 0600, function(err, fd) {
    if (err) {
      cb(err);
      return;
    }

    var writeLock = new Lock();
    writeLock.filename = filename;
    writeLock.type = isWriteLock ? 'write' : 'merge';
    writeLock.fd = fd;

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
  this._closeForWriting(function() {
    fs.unlink(that.writeLock.filename, function(err) {
      fs.close(that.writeLock.fd, function() {
        cb();
      });
    });
  });
};

Medea.prototype._closeForWriting = function(cb) {
  if (!this.active || this.active.offset === 0 || this.active.readOnly) {
    cb();
    return;
  }

  var that = this;
  this._closeHintFile(function() {
    fs.fsync(that.active.fd, function(err) {
      fs.close(that.active.fd, function(err) {
        that.active.readOnly = true;
        cb();
      });
    });
  });
};

Medea.prototype._closeHintFile = function(cb) {
  var hintFilename = this.dirname + '/' + this.active.timestamp + '.medea.hint';
  var keysz = new Buffer(sizes.keysize);
  keysz.write('0');

  var tstamp = new Buffer(sizes.timestamp);
  tstamp.write('0');

  var offset = new Buffer(sizes.offset);
  offset.write(Number.MAX_VALUE.toString());

  var totalSize = new Buffer(sizes.totalSize);
  totalSize.write(this.active.hintCrc.toString());
  
  var key = new Buffer(1);
  key.write('');

  var bufs = Buffer.concat([tstamp, keysz, totalSize, offset, key]);
  var that = this;
  this._write(hintFilename, this.active.hintFd, bufs, function() {
    fs.fsync(that.active.hintFd, function(err) {
      fs.close(that.active.hintFd, function(err) {
        that.active.hintFd = null;
        that.active.hintCrc = new Buffer(sizes.crc);
        cb();
      });
    });
  });
};

Medea.prototype.put = function(k, v, cb) {
  var that = this;
  fs.stat(this.active.filename, function(err, stat) {
    var offset = stat ? stat.size : 0;
    that._put(k, v, offset, cb);
  });
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
        var hintFilename = that.dirname + '/' + stamp + '.medea.hint';
        var hintFile = new FileInfo();
        hintFile.filename = hintFilename;
        that._open(hintFile, function(err, val2) {
          that.active = new FileInfo();
          that.active.filename = file.filename;
          that.active.readOnly = false;
          that.active.fd = val1.fd;
          that.active.hintFd = val2.fd;
          that.active.offset = 0;
          that.active.timestamp = stamp;

          cb();
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
    next = this._wrapWriteFile;
  } else if (check === writeCheck.fresh) {
    var that = this;
    next = function(cb) {
      that._acquire(that.dirname, 'write', function(err, writeLock) {
        that._createFile(function(err, file) {
          that._writeActiveFile(writeLock, that.active.filename, function() {
            cb();
          });
        });
      });
    };
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

    timestamp.write(ts.toString());
    keysz.write(key.length.toString());
    valuesz.write(value.length.toString());

    var bufs = Buffer.concat([timestamp, keysz, valuesz, key, value]);
    var crcBuf = crc32(bufs);

    crcBuf.copy(crc, 0, 0, crcBuf.length);

    var line = Buffer.concat([crc, bufs]);

    that._write(that.active.filename, that.active.fd, line);

    var offsetField = new Buffer(sizes.offset);
    var totalSizeField = new Buffer(sizes.totalSize);

    var totalSz = key.length + value.length + sizes.header;
    offsetField.write(offset.toString());
    totalSizeField.write(totalSz.toString());

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

    cb();
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
  stream.write(bufs);
  if (cb) cb();
};

Medea.prototype.get = function(key, cb) {
  var entry = this.keydir[key];
  if (entry) {
    var readBuffer = new Buffer(entry.valueSize);
    fs.read(this.active.fd, readBuffer, 0, entry.valueSize, entry.valuePosition, function(err, bytesRead, buffer) {
      if (buffer !== tombstone) {
        cb(buffer);
      } else {
        cb();
      }
    });
  } else {
    cb();
  }
};

Medea.prototype.remove = function(key, cb) {
  var that = this;
  this.put(key, tombstone, function() {
    if (that.keydir[key]) {
      delete that.keydir[key];
    }

    cb();
  });
};

Medea.prototype.sync = function(cb) {
  fs.fsync(this.active.fd, cb);
};
