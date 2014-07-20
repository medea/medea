var fs = require('fs');
var bufferEqual = require('buffer-equal')
var crc32 = require('buffer-crc32');
var constants = require('./constants');
var fileops = require('./fileops');
var DataBuffer = require('./data_buffer');
var DataEntry = require('./data_entry');
var DataFile = require('./data_file');
var DataFileParser = require('./data_file_parser');
var HintFileParser = require('./hint_file_parser');
var KeyDirEntry = require('./keydir_entry');
var Lock = require('./lock');
var WriteBatch = require('./write_batch');
var Snapshot = require('./snapshot');

var sizes = constants.sizes;
var headerOffsets = constants.headerOffsets;
var writeCheck = constants.writeCheck;

var tombstone = new Buffer('medea_tombstone');
var isTombstone = function (buffer) {
  return bufferEqual(buffer, tombstone)
}

/*var FileStatus = function() {
  this.filename = null;
  this.fragmented = null;
  this.deadBytes = null;
  this.totalBytes = null;
  this.oldestTimestamp = null;
  this.newestTimestamp = null;
};*/

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
  this.readableFiles = [];
  this.fileReferences = {};
};

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

  var that = this;
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

Medea.prototype._closeReadableFiles = function(cb) {
  var that = this;
  this.readableFiles.forEach(function(f) {
    if (f.fd !== that.active.fd) {
      fs.closeSync(f.fd);
    }
  });
};

Medea.prototype.close = function(cb) {
  var that = this;
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
  var file = this.active;

  if (!(k instanceof Buffer)) {
    k = new Buffer(k.toString());
  }

  if (!(v instanceof Buffer) && typeof v !== 'string') {
    v = new Buffer(v.toString());
  }

  var bytesToBeWritten = sizes.header + k.length + v.length;
  this.bytesToBeWritten += bytesToBeWritten;

  var that = this;
  var next = function(cb) { cb(null, file); };
  var check = this._checkWrite();
  if (check === writeCheck.wrap) {
    next = function(cb) {
      var file = that._wrapWriteFileSync(file);
      cb(null, file);
    };
  }

  next(function(err, file) {
    var key = k;
    var value = v;
    var ts = Date.now();
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

  var file = this.active;

  var batchBuffers = [];
  var batchSize = 0;

  batch.operations.forEach(function(operation) {
    var entry = operation.entry;
    var buffer = DataBuffer.fromKeyValuePair(entry.key, entry.value);
    batchBuffers.push(buffer);
    batchSize += buffer.length;
  });

  var bytesToBeWritten = batchSize;
  this.bytesToBeWritten += bytesToBeWritten;

  var that = this;
  var next = function(cb) { cb(null, file); };
  var check = this._checkWrite();
  if (check === writeCheck.wrap) {
    next = function(cb) {
      var file = that._wrapWriteFileSync(file);
      cb(null, file);
    };
  }

  next(function(err, file) {
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

          if (operation.type === 'remove')
            delete that.keydir[key];
          else
            that.keydir[key] = keydirDelta[key];
        })

        if (cb) cb();
      });
    });
  });
};

Medea.prototype._wrapWriteFileSync = function(oldFile) {
  var isActive = true;

  if (!oldFile) {
    oldFile = this.active;
  }

  if (oldFile.fd !== this.active.fd) {
    isActive = false;
  }

  var file = DataFile.createSync(this.dirname);

  if (isActive) {
    this.isWrapping = true;
    this.writeLock.writeActiveFileSync(this.dirname, file);
    this.active = file;
    this.readableFiles.push(file);
    oldFile.closeForWritingSync();
    this.bytesToBeWritten = 0;
  } else {
    this.activeMerge = file;
    oldFile.closeForWritingSync();
  }

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

      if (!isTombstone(buf)) {
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

    if (err) {
      if (cb) cb(err);
    } else {
      if(cb) cb();
    }
  });
};

Medea.prototype.createBatch = function() {
  return new WriteBatch();
};

Medea.prototype.listKeys = function(cb) {
  if (cb) cb(null, Object.keys(this.keydir));
};

var MappedItem = function() {
  this.recordKey = null;
  this.key = null;
  this.value = null;
};

Medea.prototype.createSnapshot = function () {
  return new Snapshot(this);
}

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

  that.listKeys(function(err, keys) {
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
  });
};

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
  });
};

Medea.prototype.compact = function(cb) {
  /*
   * 1. Get readable files.
   * 2. Match data file entries with keydir entries.
   * 3. If older than keydir version or if tombstoned, delete.
   * 4. Write new key to file system, update keydir.
   * 5. Delete old files.
   */

  var unlink = function(filenames, index, cb) {
    if (index === filenames.length) {
      cb();
      return;
    }

    fs.unlink(filenames[index], function(err) {
      unlink(filenames, ++index, cb);
    });
  };

  var files = this.readableFiles.slice(0).sort(function(a, b) {
    if (a.timestamp < b.timestamp) {
      return 1;
    } else if (a.timestamp > b.timestamp) {
      return - 1;
    }

    return 0;
  });

  files.shift(); // remove current file.

  this.activeMerge = DataFile.createSync(this.dirname);

  if (!files.length) {
    return cb();
  }

  var self = this;
  this._compactFile(files, 0, function(err) {
    if (err) {
      cb(err);
      return;
    }

    var dataFileNames = files
      .filter(function (f) {
        // console.log(self.fileReferences, f, !self.fileReferences[f.timestamp])
        return !self.fileReferences[f.timestamp];
      })
      .map(function(f) {
        return f.filename;
      });

    var hintFileNames = dataFileNames.map(function(filename) {
      return filename.replace('.data', '.hint');
    });

    self.sync(self.activeMerge, function(err) {
      if (err) {
        if (cb) cb(err);
        return;
      }

      unlink(dataFileNames.concat(hintFileNames), 0, function(err) {
        if (err) {
          if (cb) cb(err);
          return;
        }

        self.readableFiles.push(self.activeMerge);
        self.readableFiles = self.readableFiles.filter(function (file) {
          return fs.existsSync(file.filename);
        });

        if (cb) cb();
      });
    });
  });
};

Medea.prototype._compactFile = function(files, index, cb) {
  var self = this;
  var file = files[index];
  var parser = new DataFileParser(file);
  this.delKeyDir = [];

  parser.on('error', function(err) {
    cb(err);
  });

  parser.on('entry', function(entry) {
    var outOfDate = self._outOfDate([self.keydir, self.delKeyDir], false, entry);
    if (outOfDate) {
      // delete self.keydir[entry.key];
      return;
    }

   if (!isTombstone(entry.value)) {
     var newEntry = new KeyDirEntry();
     newEntry.valuePosition = entry.valuePosition;
     newEntry.valueSize = entry.valueSize;
     newEntry.fileId = entry.fileId;
     newEntry.timestamp = entry.timestamp;

     self.delKeyDir[entry.key] = newEntry;

     delete self.keydir[entry.key];
     self._innerMergeWrite(entry);
   } else {
     if (self.delKeyDir[entry.key]) {
       delete self.delKeyDir[entry.key];
     }

     self._innerMergeWrite(entry);
   } 
  });

  parser.on('end', function() {
    if (files.length === index + 1) {
      cb(null);
    } else {
      self._compactFile(files, ++index, cb);
    }
  });

  parser.parse(cb);
};

Medea.prototype._outOfDate = function(keydirs, everFound, fileEntry) {
  var self = this;

  if (!keydirs.length) {
   return (!everFound);
  }

  var keydir = keydirs[0];
  var keyDirEntry = keydir[fileEntry.key];

  if (!keyDirEntry) {
    keydirs.shift();
    return self._outOfDate(keydirs, everFound, fileEntry);
  }

  if (keyDirEntry.timestamp === fileEntry.timestamp) {
    if (keyDirEntry.fileId > fileEntry.fileId) {
      return true;
    } else if (keyDirEntry.fileId === fileEntry.fileId) {
      if (keyDirEntry.offset > fileEntry.offset) {
        return true;
      } else {
        keydirs.shift();
        return self._outOfDate(keydirs, true, fileEntry);
      }
    } else {
      keydirs.shift();
      return self._outOfDate(keydirs, true, fileEntry);
    }
  } else if (keyDirEntry.timestamp < fileEntry.timestamp) {
    keydirs.shift();
    return self._outOfDate(keydirs, true, fileEntry);
  }

  return true;
};

Medea.prototype._innerMergeWrite = function(dataEntry, outfile, cb) {
  var file = this.activeMerge;
  var buf = dataEntry.buffer;
  var bytesToBeWritten = buf.length;

  var that = this;

  var next = function(cb) { cb(null, file); };
  var check = this._checkWrite();
  if (check === writeCheck.wrap) {
    next = function(cb) {
      var file = that._wrapWriteFileSync(file);
      cb(null, file);
    };
  }

  var that = this;
  next(function(err, file) {
    /**
     * [crc][timestamp][keysz][valuesz][key][value]
     */
    var key = dataEntry.key;
    var value = dataEntry.value;
    var lineBuffer = dataEntry.buffer;

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
      key.copy(hintBufs, sizes.timestamp + sizes.keysize + sizes.totalsize + sizes.offset);

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
        entry.timestamp = dataEntry.timesamp;


        fs.fsync(file.fd, function(err) {
          if (err) {
            if (cb) return cb(err);
          }

          that.keydir[key] = entry;

          if (cb) cb();
        });
      });
    });
  });
};
