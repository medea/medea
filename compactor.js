var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var path = require('path');
var rimraf = require('rimraf');

var async = require('async');
var crc32 = require('buffer-crc32');
var Map = global.Map || require('es6-map');

var constants = require('./constants');
var utils = require('./utils');
var DataFile = require('./data_file');
var DataFileParser = require('./data_file_parser');
var KeyDirEntry = require('./keydir_entry');

var headerOffsets = constants.headerOffsets;
var sizes = constants.sizes;

var Compactor = module.exports = function (db) {
  EventEmitter.call(this);
  this.db = db;
  this.activeMerge = null;
  this.delKeyDir = null;
  this.bytesToBeWritten = 0;
  this.gettingNewActiveFile = false;
}
require('util').inherits(Compactor, EventEmitter);

/*
 * 1. Get readable files.
 * 2. Match data file entries with keydir entries.
 * 3. If older than keydir version or if tombstoned, delete.
 * 4. Write new key to file system, update keydir.
 * 5. Delete old files.
 */
Compactor.prototype.compact = function (cb) {
  var self = this;
  var fileReferences = Object.keys(this.db.fileReferences).map(Number);
  var files = this.db.readableFiles
    .filter(function (file) {
      return fileReferences.indexOf(file.timestamp) === -1 &&
        file.timestamp !== self.db.active.timestamp;
    })
    .sort(function(a, b) {
      if (a.timestamp < b.timestamp) {
        return 1;
      } else if (a.timestamp > b.timestamp) {
        return - 1;
      }

      return 0;
    });

  if (!files.length) {
    return cb();
  }

  // activeMerge === null means that when _getActiveMerge is called a new
  // merge file will be created
  this.activeMerge = null;

  this._compactFiles(files, function(err) {
    if (err || !self.activeMerge) {
      cb(err);
      return;
    }

    self.db.sync(self.activeMerge, function(err) {

      if (err) {
        if (cb) cb(err);
        return;
      }

      self._cleanUpDanglingHintFiles(function(err) {
        if (err) {
          if (cb) cb(err);
          return;
        }

        if (cb) cb();
      });
    });
  });
};

Compactor.prototype._cleanUpDanglingHintFiles = function(cb) {
  var hintFileNames = this.db.readableFiles.map(function(file) {
    return file.filename.replace('.data', '.hint');
  });

  var hintFileRegex = /\.hint$/;

  var self = this;
  fs.readdir(this.db.dirname, function(err, files) {
    if (err) {
      cb(err);
      return;
    }

    var diff = files
      .map(function(filename) {
        return path.join(self.db.dirname, filename);
      })
      .filter(function(filename) {
        return hintFileRegex.test(filename) && hintFileNames.indexOf(filename) === -1;
      });

    var index = 0;
    var max = diff.length;

    function unlink(i) {
      if (i === max) {
        cb();
        return;
      }

      var filename = diff[i];

      fs.stat(filename, function(err) {
        if (err && err.code === 'EPERM' && process.platform === 'win32') {
          unlink(++i);
          return;
        }

        rimraf(filename, function(err) {
          if (err) {
            cb(err);
            return;
          }

          unlink(++i);
        });
      });
    }

    unlink(index);
  });
};

Compactor.prototype._handleEntries = function (entries, cb) {
  var self = this;

  async.forEachSeries(
    entries,
    function (entry, done) {
      self._handleEntry(entry, done);
    },
    cb
  );
}

Compactor.prototype._handleEntry = function (entry, cb) {
  var self = this;

  var outOfDate = self._outOfDate([self.db.keydir, self.delKeyDir], false, entry);
  if (outOfDate) {
    // Use setImmediate to avoid "Maximum call stack size exceeded" if we're running
    // compaction on multiple outdated entries
    return setImmediate(cb);
  }

  if (!utils.isTombstone(entry.value)) {
    var newEntry = new KeyDirEntry();
    newEntry.valuePosition = entry.valuePosition;
    newEntry.valueSize = entry.valueSize;
    newEntry.fileId = entry.fileId;
    newEntry.timestamp = entry.timestamp;

    self.delKeyDir.set(entry.key.toString(), newEntry);

    self.db.keydir.delete(entry.key.toString());
    self._innerMergeWrite(entry, cb);
  } else {
    if (self.delKeyDir.has(entry.key.toString())) {
      self.delKeyDir.delete(entry.key.toString());
    }

    self._innerMergeWrite(entry, cb);
  }
};

Compactor.prototype._compactFiles = function (files, cb) {
  var self = this;

  async.forEachSeries(
    files,
    function (file, done) {
      self._compactFile(file, done);
    },
    cb
  );
}

Compactor.prototype._compactFile = function(file, cb) {
  var self = this;
  var parser = new DataFileParser(file);
  var entries = [];
  this.delKeyDir = new Map();

  parser.on('error', function(err) {
    cb(err);
  });

  parser.on('entry', function(entry) {
    entries.push(entry);
  });

  parser.on('end', function() {
    self._handleEntries(entries, function (err) {
      if (err) {
        return cb(err);
      }

      var index = self.db.readableFiles.indexOf(file);
      self.db.readableFiles.splice(index, 1)

      fs.close(file.fd, function(err) {
        if (err) {
          return cb(err);
        }

        rimraf(file.filename, function (err) {
          if (err) {
            return cb(err);
          }

          rimraf(file.filename.replace('.data', '.hint'), function(err) {
            if (err && err.code !== 'ENOENT') {
              return cb(err);
            }
            cb();
          });
        });
      });
    });
  });

  parser.parse(cb);
};

Compactor.prototype._outOfDate = function(keydirs, everFound, fileEntry) {
  var self = this;

  if (!keydirs.length) {
   return (!everFound);
  }

  var keydir = keydirs[0];
  var keyDirEntry = keydir.get(fileEntry.key.toString());

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

Compactor.prototype._getActiveMerge = function (bytesToBeWritten, cb) {
  var self = this;

  if (this.activeMerge && this.bytesToBeWritten + bytesToBeWritten < this.db.maxFileSize) {
    this.bytesToBeWritten += bytesToBeWritten;
    cb(null, this.activeMerge);
  } else {
    this.once('newActiveMergeFile', function () {
      self._getActiveMerge(bytesToBeWritten, cb);
    });

    if (!this.gettingNewActiveFile) {
      this.gettingNewActiveFile = true;
      self._wrapWriteFile(function () {
        self.gettingNewActiveFile = false;
        self.emit('newActiveMergeFile');
      });
    }
  }
}

Compactor.prototype._wrapWriteFile = function(cb) {
  var self = this;
  var oldFile = this.activeMerge;

  DataFile.create(this.db.dirname, function (err, file) {
    if (err) {
      return cb(err);
    }

    self.activeMerge = file;
    self.db.readableFiles.push(file);
    self.bytesToBeWritten = 0;
    if (oldFile) {
      oldFile.closeForWriting(cb);
    } else {
      cb();
    }
  });
};

Compactor.prototype._innerMergeWrite = function(dataEntry, cb) {
  var self = this;
  var buf = dataEntry.buffer;

  this._getActiveMerge(buf.length, function (err, file) {
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
        entry.timestamp = dataEntry.timestamp;

        fs.fsync(file.fd, function(err) {
          if (err) {
            if (cb) return cb(err);
          }

          self.db.keydir.set(key.toString(), entry);

          if (cb) cb();
        });
      });
    });
  });
};
