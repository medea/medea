var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var path = require('path');
var crc32 = require('buffer-crc32');
var lockFile = require('pidlockfile');
var timestamp = require('monotonic-timestamp');
var async = require('async');
var Map = global.Map || require('es6-map');
var rimraf = require('rimraf');
var constants = require('./constants');
var fileops = require('./fileops');
var DataBuffer = require('./data_buffer');
var DataEntry = require('./data_entry');
var utils = require('./utils');
var Compactor = require('./compactor');
var DataFile = require('./data_file');
var destroy = require('./destroy');
var HintFileParser = require('./hint_file_parser');
var KeyDirEntry = require('./keydir_entry');
var MapReduce = require('./map_reduce');
var WriteBatch = require('./write_batch');
var Snapshot = require('./snapshot');

var sizes = constants.sizes;
var headerOffsets = constants.headerOffsets;
var tombstone = constants.tombstone;
var writeCheck = constants.writeCheck;

/*var FileStatus = function() {
  this.filename = null;
  this.fragmented = null;
  this.deadBytes = null;
  this.totalBytes = null;
  this.oldestTimestamp = null;
  this.newestTimestamp = null;
};*/

var Medea = function(options) {
  if (!(this instanceof Medea))
    return new Medea(options);

  EventEmitter.call(this);

  this.active = null;
  this.keydir = new Map();

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
  this.setMaxListeners(Infinity);
};

require('util').inherits(Medea, EventEmitter);

Medea.prototype.open = function(dir, options, cb) {
  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  if (typeof dir === 'function') {
    options = {};
    cb = dir;
    dir = this.dirname || process.cwd() + '/medea';
  }

  this.dirname = dir;

  var self = this;

  async.series(
    [
      function (done) {
        require('mkdirp')(dir, done);
      },
      function (done) {
        lockFile.lock(dir + '/medea.lock', done);
      },
      function (done) {
        self._scanFiles(done);
      },
      function (done) {
        if (self.readOnly) {
          cb()
        } else {
          done();
        }
      },
      function (done) {
        DataFile.create(self.dirname, function (err, file) {
          if (err) {
            return done(err);
          }

          self.active = file;
          self.readableFiles.push(self.active);

          done();
        });
      }
    ],
    cb
  );
};

Medea.prototype._scanFiles = function (cb) {
  var self = this;

  this._validateFiles(function (err, files) {
    if (err) {
      return cb(err);
    }

    fileops.listDataFiles(self.dirname, function(err, arr) {
      arr = arr.filter(function(file) {
        return files.indexOf(file) > -1;
      });

      if (err) {
        return cb(err);
      }

      self._openFiles(arr, function (err) {
        if (err) {
          return cb(err);
        }

        self._scanKeyFiles(arr, cb);
      });
    });
  });
}

Medea.prototype._validateFiles = function (cb) {
  var self = this;
  var filter = function (file) {
    var slice = file.slice(-5);
    return slice === '.data' || slice === '.hint';
  }

  fs.readdir(this.dirname, function(err, files) {
    if (err) {
      cb(err);
      return;
    }

    var count = 0;
    var validFiles = [];

    files = files.filter(filter);

    if (files.length === 0) {
      cb(null, validFiles);
      return;
    }

    files.forEach(function(file) {
      file = path.join(self.dirname, file);
      fs.stat(file, function(err, stat) {
        if (stat && !stat.isFile()) {
          count++;
          if (files.length === count) {
            cb(null, validFiles);
          }
        } else if (stat && stat.size === 0 || (err && err.code === 'EPERM' && process.platform === 'win32')) { // Windows EPERM issue: https://github.com/medea/medea/issues/51
          rimraf(file, function(err) {
            count++;
            if (files.length === count) {
	            if (err && err.code === 'EPERM' && process.platform === 'win32') {
	              cb(null, validFiles);
	            } else {
                cb(err,validFiles);
              }
            }
          });
        } else if (err) {
          cb(err);
        } else {
          count++;
          validFiles.push(file);
          if (files.length === count) {
            cb(err, validFiles);
          }
        }
      });
    });
  });
}

Medea.prototype._openFiles = function (filenames, cb) {
  var self = this;

  async.forEach(
    filenames,
    function (f, done) {
      fs.open(f, 'r', function (err, fd) {
        if (err) {
          return done(err);
        }

        var readable = new DataFile();
        readable.fd = fd;
        readable.filename = f;
        readable.dirname = self.dirname;

        var filename = f.split(path.sep);
        readable.timestamp = filename[filename.length - 1].split('.')[0];
        readable.timestamp = Number(readable.timestamp);

        self.readableFiles.push(readable);
        done(null)
      });
    },
    cb
  );
};

Medea.prototype._scanKeyFiles = function(arr, cb) {
  HintFileParser.parse(this.dirname, arr, this.keydir, cb);
};

Medea.prototype._closeReadableFiles = function(cb) {
  var self = this;
  var tasks = this.readableFiles
    .filter(function (file) {
      return file.fd !== self.active.fd
    })
    .map(function (file) {
      return function (done) {
        fs.close(file.fd, done);
      }
    });

  async.parallel(tasks, cb);
};

Medea.prototype.close = function(cb) {
  var self = this;

  this.active.closeForWriting(function() {
    lockFile.unlock(self.dirname + '/medea.lock', function () {
      self._closeReadableFiles(function () {
        fs.close(self.active.fd, function(err) {
          self.active = null;
          cb(err);
        });
      });
    });
  });
};

Medea.prototype.put = function(k, v, cb) {
  if (!(k instanceof Buffer)) {
    k = new Buffer(k.toString());
  }

  if (!(v instanceof Buffer)) {
    v = new Buffer(v.toString());
  }

  var bytesToBeWritten = sizes.header + k.length + v.length;

  var self = this;

  this._getActiveFile(bytesToBeWritten, function (err, file) {
    if (err) {
      return cb(err);
    }

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

        self.keydir.set(k.toString(), entry);
        if (cb) cb();
      });
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
  var self = this;

  this._getActiveFile(bytesToBeWritten, function (err, file) {
    if (err) {
      return cb(err);
    }

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
      var keydirDelta = new Map();

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

        keydirDelta.set(key.toString(), entry);

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
            self.keydir.delete(key);
          } else {
            self.keydir.set(key, keydirDelta.get(key));
          }
        })

        if (cb) cb();
      });
    });
  });
};

Medea.prototype._getActiveFile = function (bytesToBeWritten, cb) {
  var self = this;

  if (!this.active) {
    return cb(new Error('Database not open'));
  }

  if (this.bytesToBeWritten + bytesToBeWritten < this.maxFileSize) {
    this.bytesToBeWritten += bytesToBeWritten;
    cb(null, this.active);
  } else {
    this.once('newActiveFile', function (err) {
      if (err) {
        return cb(err);
      }

      self._getActiveFile(bytesToBeWritten, cb);
    });

    if (!this.gettingNewActiveFile) {
      this.gettingNewActiveFile = true;
      self._wrapWriteFile(function (err) {
        self.gettingNewActiveFile = false;
        self.emit('newActiveFile', err);
      });
    }
  }
}

Medea.prototype._wrapWriteFile = function (cb) {
  var oldFile = this.active;
  var self = this;
  DataFile.create(this.dirname, function (err, file) {
    if (err) {
      return cb(err);
    }

    self.active = file;
    self.readableFiles.push(file);
    self.bytesToBeWritten = 0;
    oldFile.closeForWriting(cb);
  });
}

Medea.prototype.get = function(key, snapshot, cb) {
  if (!cb) {
    cb = snapshot;
    snapshot = undefined;
  }

  if (snapshot && snapshot.closed) {
    return cb(new Error('Snapshot is closed'));
  }

  if (!this.active) {
    return cb(new Error('Database not open'));
  }
  key = key.toString()
  var entry = snapshot ? snapshot.keydir.get(key) : this.keydir.get(key);
  if (!entry || entry.valueSize === 0) {
    if (cb) cb();
    return;
  }

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
  fs.read(fd, buf, 0, entry.valueSize, entry.valuePosition, function(err) {
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
};

Medea.prototype.remove = function(key, cb) {
  var self = this;
  this.put(key, tombstone, function(err) {
    if (err) {
      if (cb) cb(err);
    } else {
      if (self.keydir.has(key)) {
        self.keydir.delete(key);
      }
      if(cb) cb();
    }
  });
};

Medea.prototype.createBatch = function() {
  return new WriteBatch();
};

Medea.prototype.listKeys = function(cb) {
  if (cb) cb(null, utils.dumpMapKeys(this.keydir));
};

Medea.prototype.createSnapshot = function () {
  if (!this.active) {
    throw new Error('Database not open');
  }

  return new Snapshot(this);
}

Medea.prototype.sync = function(file, cb) {
  if (!cb && typeof file === 'function') {
    cb = file;
    file = this.active;
  }

  if (!file) {
    return cb(new Error('Database not open'));
  }

  fs.fsync(file.dataStream.fd, function(err) {
    if (err) {
      cb(err);
      return;
    }

    fs.fsync(file.hintStream.fd, cb);
  });
};

Medea.prototype.compact = function(cb) {
  if (!this.active) {
    return cb(new Error('Database not open'));
  }

  this.compactor.compact(cb);
};

Medea.prototype.mapReduce = function(options, cb) {
  if (!this.active) {
    return cb(new Error('Database not open'));
  }

  var job = new MapReduce(this, options);
  job.run(cb);
};

Medea.prototype.destroy = function (cb) {
  destroy(this.dirname, cb);
};

Medea.destroy = destroy;

module.exports = Medea;
