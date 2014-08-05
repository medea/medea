var fs = require('fs');
var appendStream = require('append-stream');
var parallel = require('async').parallel;
var constants = require('./constants');
var fileops = require('./fileops');
var sizes = constants.sizes;

var DataFile = module.exports = function() {
  this.dirname = null;
  this.filename = null;
  this.fd = null;
  this.offset = 0;
  this.hintFd = null;
  this.readOnly = true;
  this.hintCrc = new Buffer(sizes.crc);
  this.timestamp = null;
  this.writeLock = null;
  this.closingHintFile = false;
  this.dataStream = null;
  this.hintStream = null;
};

DataFile.create = function(dirname, cb) {
  fileops.mostRecentTstamp(dirname, function(err, stamp) {
    stamp = stamp + 1;
    var filename = dirname + '/' + stamp + '.medea.data';
    var file = new DataFile();
    file.filename = filename;
    file.dirname = dirname;
    file.readOnly = false;
    file.timestamp = stamp;

    var hintFilename = dirname + '/' + stamp + '.medea.hint';

    parallel({
        dataStream: function (done) {
          appendStream(filename, done);
        },
        hintStream: function (done) {
          appendStream(hintFilename, done);
        }
      },
      function (err, results) {
        if (err) {
          return cb(err);
        }

        file.dataStream = results.dataStream;
        file.hintStream = results.hintStream;

        parallel({
            dataFd: function (done) {
              fs.open(file.filename, 'r', done);
            },
            hintFd: function (done) {
              fs.open(hintFilename, 'r', done);
            }
          },
          function (err, results) {
            if (err) {
              return cb(err);
            }

            file.fd = results.dataFd;
            file.hintFd = results.hintFd;

            cb(null, file);
          }
        ); 
      }
    );
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

  var self = this;
  this.hintStream.write(this.hintCrc, function() {
    fs.fsync(self.hintFd, function(err) {
      if (err) {
        //console.log('Error fsyncing hint file during close.', err);
        if (cb) cb(err);
        return;
      }
      self.hintStream.end(function(err) {
        self.hintFd = null;
        self.hintCrc = new Buffer(sizes.crc);
        if (cb) cb();
      });
    });
  });
};
