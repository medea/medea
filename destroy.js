var fs = require('fs');
var path = require('path');

var async = require('async');

var destroy = function (directory, cb) {
  fs.readdir(directory, function (err, files) {
    if (err) {
      return cb(err);
    }

    files = files
      .filter(function (fileName) {
        var extname = path.extname(fileName);
        return extname === '.hint' || extname === '.data' || fileName === 'medea.lock';
      })
      .map(function (fileName) {
        return path.join(directory, fileName);
      })

    async.forEach(files, fs.unlink, function (err) {
      if (err) {
        return cb(err);
      }

      fs.rmdir(directory, function (err) {
        cb(err && err.code !== 'ENOTEMPTY' ? err : undefined);
      });
    });
  });
}

module.exports = destroy