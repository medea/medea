var fs = require('fs');
var EventEmitter = require('events').EventEmitter;
var crc32 = require('buffer-crc32');
var timestamp = require('monotonic-timestamp');
var util = require('util');

var constants = require('./constants');
var fileops = require('./fileops');
var DataBuffer = require('./data_buffer');
var DataEntry = require('./data_entry');
var utils = require('./utils');
var Compactor = require('./compactor');
var DataFile = require('./data_file');
var HintFileParser = require('./hint_file_parser');
var KeyDirEntry = require('./keydir_entry');
var Lock = require('./lock');
var MapReduce = require('./map_reduce');
var WriteBatch = require('./write_batch');
var Snapshot = require('./snapshot');

var sizes = constants.sizes;
var headerOffsets = constants.headerOffsets;
var tombstone = constants.tombstone;
var writeCheck = constants.writeCheck;

var Medea = function(options) {
  if (!(this instanceof Medea))
    return new Medea(options);

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
  this.readableFiles = [];
  this.fileReferences = {};
  this.compactor = new Compactor(this);

  EventEmitter.call(this);
};
util.inherits(Medea, EventEmitter);

Medea.prototype.open = function(dir, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = {};
  }

  if (typeof dir === 'function') {
    options = {};
    callback = dir;
    dir = this.dirname || process.cwd() + '/medea';
  }

  var that = this;

  var cb = function(err) {
    if (!err) {
      that.emit('open');
    }

    if (callback) {
      callback(err);
    }
  };

  this.dirname = dir;

  var scanFiles = function(cb) {
    that._getReadableFiles(function(err, arr) {
      arr.forEach(function(f) {
        var fd = fs.openSync(f, 'r');
        var readable = new DataFile();
        readable.fd = fd;
        readable.filename = f;
        readable.dirname = that.dirname;

        var filename = f.replace('\\', '/').split('/');
        readable.timestamp = filename[filename.length - 1].split('.')[0];
        readable.timestamp = Number(readable.timestamp);

        that.readableFiles.push(readable);
      });
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
              that.readableFiles.push(that.active);
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
      fileops.listDataFiles(that.dirname, writeFile, mergeFile, function(err, files) {
        cb(err, files);
      });
    });
  });
};


Medea.prototype._acquire = function(dir, type, cb) {
  var filename = dir + '/medea.' + type + '.lock';

  var that = this;
  var writeFile = function() {
    Lock.acquire(filename, true, function(err, writeLock) {
      that.writeLock = writeLock;
      that.writeLock.writeActiveFile(that.dirname, null, function(err) {
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

Medea.prototype._closeReadableFiles = function(cb) {
  var that = this;
  this.readableFiles.forEach(function(f) {
    if (f.fd !== that.active.fd) {
      fs.closeSync(f.fd);
    }
  });
};

Medea.prototype.close = function(callback) {
  var that = this;

  var cb = function(err) {
    if (!err) {
      that.emit('close');
    }

    if (callback) {
      callback(err);
    }
  };

  this.active.closeForWriting(function() {
    if (that.active.offset === 0 && that.bytesToBeWritten === 0) {
      fs.unlink(that.active.filename, function(err) {
        fs.close(that.active.fd, function() {
          fs.unlink(that.active.filename.replace('.data', '.hint'), function(err) {
            fs.unlink(that.writeLock.filename, function(err) {
              fs.close(that.writeLock.fd, function() {
                that._closeReadableFiles();
                if (cb) cb();
              });
            });
          });
        });
      });
    } else {
      fs.unlink(that.writeLock.filename, function(err) {
        fs.close(that.writeLock.fd, function() {
          that._closeReadableFiles();
          if (cb) cb();
        });
      });
    }
  });
};

Medea.prototype.put = function(k, v, cb) {
  if (!(k instanceof Buffer)) {
    k = new Buffer(k.toString());
  }

  if (!(v instanceof Buffer) && typeof v !== 'string') {
    v = new Buffer(v.toString());
  }

  var bytesToBeWritten = sizes.header + k.length + v.length;
  var file = this._getActiveFile(bytesToBeWritten);

  var that = this;

  var key = k;
  var value = v;
  var ts = timestamp();
  var lineBuffer = DataBuffer.fromKeyValuePair(key, value, ts);

  file.write(lineBuffer, function(err) {
    if (err) {
      if (cb) cb(err);
      return;
    }

    var oldOffset = file.offset;
    file.offset += lineBuffer.length;

    var totalSz = key.length + value.length + sizes.header;

    var hintBufs = new Buffer(sizes.timestamp + sizes.keysize + sizes.offset + sizes.totalsize + key.length)

    //timestamp
    lineBuffer.copy(hintBufs, 0, headerOffsets.timestamp, headerOffsets.timestamp + sizes.timestamp);
    //keysize
    lineBuffer.copy(hintBufs, sizes.timestamp, headerOffsets.keysize, headerOffsets.keysize + sizes.keysize);
    //total size
    hintBufs.writeUInt32BE(totalSz, sizes.timestamp + sizes.keysize);
    //offset
    hintBufs.writeDoubleBE(oldOffset, sizes.timestamp + sizes.keysize + sizes.totalsize);
    //key
    k.copy(hintBufs, sizes.timestamp + sizes.keysize + sizes.totalsize + sizes.offset);

    file.writeHintFile(hintBufs, function(err) {
      if (err) {
        if (cb) cb(err);
        return;
      }
      file.hintCrc = crc32(hintBufs, file.hintCrc);
      file.hintOffset += hintBufs.length;

      var entry = new KeyDirEntry();
      entry.fileId = file.timestamp;
      entry.valueSize = value.length;
      entry.valuePosition = oldOffset + sizes.header + key.length;
      entry.timestamp = ts;

      that.keydir[k] = entry;

      if (utils.isTombstone(value)) {
        that.emit('remove', key);
      } else {
        that.emit('put', key, value);
      }

      if (cb) {
        cb();
      }
    });
  });
};

Medea.prototype.write = function(batch, options, cb) {
  if (typeof options === 'function') {
    cb = options;
    options = null;
  }

  options = options || {};
  options.sync = options.sync || true;


  var batchBuffers = [];
  var batchSize = 0;

  batch.operations.forEach(function(operation) {
    var entry = operation.entry;
    var buffer = DataBuffer.fromKeyValuePair(entry.key, entry.value, timestamp());
    batchBuffers.push(buffer);
    batchSize += buffer.length;
  });

  var bytesToBeWritten = batchSize;
  var file = this._getActiveFile(bytesToBeWritten);
  var that = this;

  var lineBuffer = Buffer.concat(batchBuffers, batch.size);

  file.write(lineBuffer, { sync: options.sync }, function(err) {
    if (err) {
      if (cb) cb(err);
      return;
    }

    var oldOffset = file.offset;
    file.offset += lineBuffer.length;

    var dataEntries = batchBuffers.map(function(buffer) {
      return DataEntry.fromBuffer(buffer);
    });
    var hintBufs = [];
    var hintBufsSize = 0;
    var newHintCrc = file.hintCrc;
    var keydirDelta = {};

    var copyBufferOffset = 0;

    dataEntries.forEach(function(dataEntry) {
      var key = dataEntry.key;
      var value = dataEntry.value;

      var totalSz = dataEntry.keySize + dataEntry.valueSize + sizes.header;

      var hintBuf = new Buffer(sizes.timestamp + sizes.keysize + sizes.offset + sizes.totalsize + key.length)

      //timestamp
      lineBuffer.copy(hintBuf, 0,
        copyBufferOffset + headerOffsets.timestamp,
        copyBufferOffset + headerOffsets.timestamp + sizes.timestamp);

      //keysize
      lineBuffer.copy(hintBuf, sizes.timestamp,
        copyBufferOffset + headerOffsets.keysize,
        copyBufferOffset + headerOffsets.keysize + sizes.keysize);

      //total size
      hintBuf.writeUInt32BE(totalSz, sizes.timestamp + sizes.keysize);
      //offset
      hintBuf.writeDoubleBE(oldOffset, sizes.timestamp + sizes.keysize + sizes.totalsize);
      //key
      key.copy(hintBuf, sizes.timestamp + sizes.keysize + sizes.totalsize + sizes.offset);

      hintBufsSize += hintBuf.length;
      hintBufs.push(hintBuf);
      newHintCrc = crc32(hintBuf, newHintCrc);
      
      var entry = new KeyDirEntry();
      entry.fileId = file.timestamp;
      entry.valueSize = value.length;
      entry.valuePosition = oldOffset + sizes.header + key.length;
      entry.timestamp = dataEntry.timestamp;

      keydirDelta[key] = entry;

      oldOffset += dataEntry.buffer.length;
      copyBufferOffset += dataEntry.buffer.length;
    });

    var hintBuffersToBeWritten = Buffer.concat(hintBufs, hintBufsSize);
    file.writeHintFile(hintBuffersToBeWritten, function(err) {
      if (err) {
        if (cb) cb(err);
        return;
      }

      file.hintCrc = newHintCrc;
      file.hintOffset += hintBufsSize;

      batch.operations.forEach(function (operation) {
        var key = operation.entry.key.toString();

        if (operation.type === 'remove') {
          delete that.keydir[key];
        } else {
          that.keydir[key] = keydirDelta[key];
        }
      })

      that.emit('write', batch);
      if (cb) cb();
    });
  });
};

Medea.prototype._getActiveFile = function (bytesToBeWritten) {
  var active;

  if (this.bytesToBeWritten + bytesToBeWritten < this.maxFileSize) {
    active = this.active
  } else {
    active = this._wrapWriteFileSync()
  }

  this.bytesToBeWritten += bytesToBeWritten;
  return active;
}

Medea.prototype._wrapWriteFileSync = function() {
  var oldFile = this.active;
  var file = DataFile.createSync(this.dirname);

  this.writeLock.writeActiveFileSync(this.dirname, file);
  this.active = file;
  this.readableFiles.push(file);
  oldFile.closeForWritingSync();
  this.bytesToBeWritten = 0;

  return file;
};

Medea.prototype.get = function(key, snapshot, cb) {
  if (!cb) {
    cb = snapshot;
    snapshot = undefined;
  }

  if (snapshot && snapshot.closed)
    return cb(new Error('Snapshot is closed'));

  var entry = snapshot ? snapshot.keydir[key] : this.keydir[key];
  if (entry) {
    var filename = this.dirname + '/' + entry.fileId + '.medea.data';
    var fd;
    this.readableFiles.forEach(function(df) {
      if (df.timestamp === entry.fileId) {
        fd = df.fd;
      }
    });

    if (!fd) {
      cb(new Error('Invalid file ID.'));
      return;
    }

    var buf = new Buffer(entry.valueSize);
    fs.read(fd, buf, 0, entry.valueSize, entry.valuePosition, function(err, bytesRead) {
      if (err) {
        cb(err);
        return;
      }

      if (!utils.isTombstone(buf)) {
        cb(null, buf);
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
  this.put(key, tombstone, function(err) {
    if (that.keydir[key]) {
      delete that.keydir[key];
    }

    if (cb) {
      cb(err);
    }
  });
};

Medea.prototype.createBatch = function() {
  return new WriteBatch();
};

Medea.prototype.listKeys = function(cb) {
  if (cb) cb(null, Object.keys(this.keydir));
};

Medea.prototype.createSnapshot = function () {
  return new Snapshot(this);
}

Medea.prototype.sync = function(file, cb) {
  if (!cb && typeof file === 'function') {
    cb = file;
    file = this.active;
  }

  var that = this;
  fs.fsync(file.fd, function(err) {
    if (err) {
      cb(err);
      return;
    }

    fs.fsync(file.hintFd, cb);
    that.emit('sync');
  });
};

Medea.prototype.compact = function(callback) {
  var that = this;
  var cb = function(err) {
    if (!err) {
      that.emit('compact');
    }

    callback(err);
  };

  this.compactor.compact(cb);
};

Medea.prototype.mapReduce = function(options, cb) {
  var job = new MapReduce(this, options);
  job.run(cb);
};

module.exports = Medea;
