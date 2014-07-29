var EventEmitter = require('events').EventEmitter;
var fs = require('fs');

var crc32 = require('buffer-crc32');

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

  DataFile.create(this.db.dirname, function (err, file) {
    if (err) {
      return cb(err);
    }

    self.activeMerge = file;

    self.db.readableFiles.push(self.activeMerge)

    self._compactFile(files, 0, function(err) {
      if (err) {
        cb(err);
        return;
      }

      var dataFileNames = files.map(function(f) {
        return f.filename;
      });

      var hintFileNames = files.map(function(f) {
        return f.filename.replace('.data', '.hint');
      });

      self.db.sync(self.activeMerge, function(err) {
        if (err) {
          if (cb) cb(err);
          return;
        }

        self.db.readableFiles = self.db.readableFiles
          .filter(function (file) {
            return files.indexOf(file) === -1;
          });

        self._unlink(dataFileNames.concat(hintFileNames), 0, function(err) {
          if (err) {
            if (cb) cb(err);
            return;
          }

          if (cb) cb();
        });
      });
    });
  });
}

Compactor.prototype._unlink = function(filenames, index, cb) {
  var self = this;

  fs.unlink(filenames[index], function(err) {
    if (index === filenames.length - 1) {
      cb();
      return;
    }

    self._unlink(filenames, ++index, cb);
  });
}

Compactor.prototype._handleEntry = function (entries, index, cb) {
  var self = this;

  if (index === entries.length) {
    return cb();
  }

  var entry = entries[index];
  var outOfDate = self._outOfDate([self.db.keydir, self.delKeyDir], false, entry);
  if (outOfDate) {
     // Use setImmediate to avoid "Maximum call stack size exceeded" if we're running
     // compaction on multiple outdated entries
     return setImmediate(function () {
      self._handleEntry(entries, ++index, cb);
    });
  }

  if (!utils.isTombstone(entry.value)) {
    var newEntry = new KeyDirEntry();
    newEntry.valuePosition = entry.valuePosition;
    newEntry.valueSize = entry.valueSize;
    newEntry.fileId = entry.fileId;
    newEntry.timestamp = entry.timestamp;

    self.delKeyDir[entry.key] = newEntry;

    delete self.db.keydir[entry.key];
    self._innerMergeWrite(entry, function () {
      self._handleEntry(entries, ++index, cb);
    });
  } else {
    if (self.delKeyDir[entry.key]) {
      delete self.delKeyDir[entry.key];
    }

    self._innerMergeWrite(entry, function () {
      self._handleEntry(entries, ++index, cb);
    });
  }
};

Compactor.prototype._compactFile = function(files, index, cb) {
  var self = this;
  var file = files[index];
  var parser = new DataFileParser(file);
  var entries = [];
  this.delKeyDir = [];

  parser.on('error', function(err) {
    cb(err);
  });

  parser.on('entry', function(entry) {
    entries.push(entry);
  });

  parser.on('end', function() {
    self._handleEntry(entries, 0, function () {
      if (files.length === index + 1) {
        cb(null);
      } else {
        self._compactFile(files, ++index, cb);
      }
    })
  });

  parser.parse(cb);
};

Compactor.prototype._outOfDate = function(keydirs, everFound, fileEntry) {
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

Compactor.prototype._getActiveMerge = function (bytesToBeWritten, cb) {
  var that = this;

  if (this.bytesToBeWritten + bytesToBeWritten < this.db.maxFileSize) {
    this.bytesToBeWritten += bytesToBeWritten;
    cb(null, this.activeMerge);
  } else {
    this.once('newActiveMergeFile', function () {
      that._getActiveMerge(bytesToBeWritten, cb);
    });

    if (!this.gettingNewActiveFile) {
      this.gettingNewActiveFile = true;
      that._wrapWriteFile(function () {
        that.gettingNewActiveFile = false;
        that.emit('newActiveMergeFile');
      });
    }
  }
}

Compactor.prototype._wrapWriteFile = function(cb) {
  var that = this;
  var oldFile = this.activeMerge;

  DataFile.create(this.db.dirname, function (err, file) {
    if (err) {
      return cb(err);
    }

    that.activeMerge = file;
    that.db.readableFiles.push(file);
    that.bytesToBeWritten = 0;
    oldFile.closeForWriting(cb);
  });
};

Compactor.prototype._innerMergeWrite = function(dataEntry, cb) {
  var that = this;
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

          that.db.keydir[key] = entry;

          if (cb) cb();
        });
      });
    });
  });
};