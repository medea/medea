var fs = require('fs');
var path = require('path');

var async = require('async');

var destroy = function (directory, cb) {
  fs.readdir(directory, function (err, files) {
    if (err) {
      return cb(err);
    }

    files = files.map(function (fileName) {
      return path.join(directory, fileName);
    });

    async.forEach(files, fs.unlink, function (err) {
      if (err) {
        return cb(err);
      }

      fs.rmdir(directory, cb);
    });
  });
}

module.exports = destroy