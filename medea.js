var fs = require('fs');
var crc32 = require('buffer-crc32');
var constants = require('./constants');
var fileops = require('./fileops');
var DataFile = require('./data_file');
var HintFileParser = require('./hint_file_parser');
var KeyDirEntry = require('./keydir_entry');
var Lock = require('./lock');

var sizes = constants.sizes;
var writeCheck = constants.writeCheck;

var tombstone = new Buffer('medea_tombstone');

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
          DataFile.create(that.dirname, function(err, file) {
            writeLock.writeActiveFile(that.dirname, file, function() {
              that.active = file;
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

  fileops.ensureDir(dir, function(err) {
    if (err) {
      if (cb) cb(err);
    } else {
      next(dir, that.readOnly, cb);
    }
  });
};

Medea.prototype._scanKeyFiles = function(arr, cb) {
  HintFileParser.parse(this.dirname, arr, this.keydir, cb);
};

Medea.prototype._getReadableFiles = function(cb) {
  var that = this;
  var writingFile = Lock.readActiveFile(this.dirname, 'write', function(err, writeFile) {
    var mergingFile = Lock.readActiveFile(that.dirname, 'merge', function(err, mergeFile) {
      // TODO: Filter out files marked for deletion by successful merge.
      fileops.listDataFiles(that.dirname, writeFile, mergeFile, function(err, files) {
        cb(null, files);
      });
    });
  });
};


Medea.prototype._acquire = function(dir, type, cb) {
  var filename = dir + '/medea.' + type + '.lock';

  var that = this;
  var writeFile = function() {
    Lock.acquire(filename, true, function(err, writeLock) {
      if (err) {
        console.log('Error on acquiring write lock.', err);
      }
      that.writeLock = writeLock;
      that.writeLock.writeActiveFile(that.dirname, null, function(err) {
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

  Lock.acquire(filename, false, function(err, writeLock) {
    if (err) {
      if (err.code === 'ENOENT') {
        // this is okay.  move on.
        writeFile();
        return;
      }
      cb(err);
      return;
    }

    this.writeLock = writeLock;

    var stream = fs.createReadStream(filename, { fd: writeLock.fd });
    stream.on('data', ondata);
    stream.on('end', onend(writeLock.fd));
    stream.on('error', function(err) {
      cb(err);
    });

    stream.resume();
  });
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
  this.active.closeForWriting(function() {
    fs.unlink(that.writeLock.filename, function(err) {
      fs.close(that.writeLock.fd, function() {
        if (cb) cb();
      });
    });
  });
};

Medea.prototype.put = function(k, v, cb) {
  this._put(k, v, cb);
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

    file.write(line, function() {
      var oldOffset = file.offset;
      file.offset = file.offset + line.length;
      var offsetField = new Buffer(sizes.offset);
      var totalSizeField = new Buffer(sizes.totalsize);

      var totalSz = key.length + value.length + sizes.header;
      offsetField.writeDoubleBE(oldOffset, 0);
      totalSizeField.writeUInt32BE(totalSz, 0);

      var hintBufs = Buffer.concat([timestamp, keysz, totalSizeField, offsetField, key]);

      file.writeHintFile(hintBufs, function() {
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

Medea.prototype._wrapWriteFileSync = function() {
  var oldFile = this.active;
  this.isWrapping = true;
  var file = DataFile.createSync(this.dirname);
  this.writeLock.writeActiveFileSync(this.dirname, file);
  this.active = file;
  oldFile.closeForWritingSync();
  this.bytesToBeWritten = 0;
  return file;
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

Medea.prototype.listKeys = function() {
  return Object.keys(this.keydir);
};

var MappedItem = function() {
  this.recordKey = null;
  this.key = null;
  this.value = null;
};

Medea.prototype.mapReduce = function(options, cb) {
  var that = this;

  var map = options.map;
  var reduce = options.reduce;
  var group = options.hasOwnProperty('group') ? options.group : false;
  var startKey = options.startKey;
  var endKey = options.endKey;

  var mapped = [];
  var mapper = function(recordKey) {
    return function(key, val) {
      var item = new MappedItem();
      item.recordKey = recordKey;
      item.key = key;
      item.value = val;
      mapped.push(item);
    }
  };

  var keys = that.listKeys();
  var len = keys.length;

  var i = 0;
  var acc;

  var iterator = function(keys, i, len, cb1) {
    if (i < len) {
      that.get(keys[i], function(val) {
        map(keys[i], val, mapper(keys[i]));
        iterator(keys, i+1, len, cb1);
      });
    } else {
      if (!group) {
        var remapped = {};
        mapped.forEach(function(item) {
          if (!remapped[item.key]) {
            remapped[item.key] = [];
          }

          remapped[item.key].push(item.value);
        });

        Object.keys(remapped).forEach(function(key) {
          if ((!startKey || (item.key >= startKey)) &&
              (!endKey || item.key <= endKey)) {
            acc = reduce(key, remapped[key]);
          }
        });
      } else {
        mapped.forEach(function(item) {
          if ((!startKey || (item.key >= startKey)) &&
              (!endKey || item.key <= endKey)) {
            acc = reduce(item.key, item.value);
          }
        });
      }

      cb1(acc);
    }
  };

  iterator(keys, 0, len, cb);
};

Medea.prototype.sync = function(cb) {
  fs.fsync(this.active.fd, cb);
};
