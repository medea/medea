var fs = require('fs');
var constants = require('./constants');
var fileops = require('./fileops');
var sizes = constants.sizes;

var HintFile = function() {
  this.filename = null;
  this.fd = null;
  this.offset = 0;
};

var DataFile = module.exports = function() {
  this.dirname = null;
  this.filename = null;
  this.fd = null;
  this.offset = 0;
  this.hintFd = null;
  this.readOnly = true;
  this.hintCrc = new Buffer(sizes.crc);
  this.hintOffset = 0;
  this.timestamp = null;
  this.writeLock = null;
  this.closingHintFile = false;
  this.dataStream = null;
  this.hintStream = null;
};

DataFile.create = function(dirname, cb) {
  fileops.ensureDir(dirname, function(err) {
    fileops.mostRecentTstamp(dirname, function(err, stamp) {
      stamp = stamp + 1;
      var filename = dirname + '/' + stamp + '.medea.data';

      var file = new DataFile();
      file.filename = filename;

      fileops.open(file, function(err, val1) {
        if (err) {
          cb(err);
          return;
        }
        var hintFilename = dirname + '/' + stamp + '.medea.hint';
        var hintFile = new HintFile();
        hintFile.filename = hintFilename;
        fileops.open(hintFile, function(err, val2) {
          if (err) {
            cb(err)
            return;
          }

          file.dirname = dirname;
          file.readOnly = false;
          file.fd = val1.fd;
          file.dataStream = fs.createWriteStream(filename);
          file.hintFd = val2.fd;
          file.hintStream = fs.createWriteStream(hintFilename);
          file.hintOffset = 0;
          file.offset = 0;
          file.timestamp = stamp;

          if (cb) cb(null, file);
        });
      });
    });
  });
};

DataFile.prototype.write = function(bufs, options, cb) {
  var self = this;

  if (typeof options === 'function') {
    cb = options;
    options = null;
  }

  if (typeof cb === 'undefined') {
    cb = function () {};
  }

  options = options || {};
  options.sync = options.sync || false;

  this.dataStream.write(bufs, function () {
    if (options.sync) {
      fs.fsync(self.fd, cb)
    } else {
      cb();
    }
  });
};

DataFile.prototype.writeHintFile = function(bufs, cb) {
  this.hintStream.write(bufs, cb);
};

DataFile.prototype.closeForWriting = function(cb) {
  if (this.readOnly) {
    if (cb) cb();
    return;
  }

  this.readOnly = true;
  var self = this;

  this.dataStream.end(function () {
    // TODO: write to the fd that belongs to the dataStream rather than the
    // one used for reading
    fs.fsync(self.fd, function(err) {
      if (err) {
        cb(err);
        return;
      }

      self._closeHintFile(function(err) {
        if (cb) cb(err);
      });
    });
  });
};

DataFile.prototype._closeHintFile = function(cb) {
  if (!this.hintFd || this.closingHintFile) {
    if (cb) cb();
    return;
  }

  this.closingHintFile = true;
  var hintFilename = this.dirname + '/' + this.timestamp + '.medea.hint';
  
  var crcBuf = new Buffer(sizes.crc);
  this.hintCrc.copy(crcBuf, 0, 0, this.hintCrc.length);

  var that = this;
  this.hintStream.write(crcBuf, function() {
    fs.fsync(that.hintFd, function(err) {
      if (err) {
        //console.log('Error fsyncing hint file during close.', err);
        if (cb) cb(err);
        return;
      }
      that.hintStream.end(function(err) {
        that.hintFd = null;
        that.hintCrc = new Buffer(sizes.crc);
        if (cb) cb();
      });
    });
  });
};
