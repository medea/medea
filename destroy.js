var fs = require('fs');
var path = require('path');
var rimraf = require('rimraf');

var async = require('async');
var lockFile = require('pidlockfile');

var destroy = function (directory, cb) {

  lockFile.check(path.join(directory, 'medea.lock'), function (err, locked) {
    if (!err && locked) err = new Error('database is open');

    if (err && err.code !== 'ENOENT') {
      return cb(err)
    }

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

      async.forEach(files, rimraf, function (err) {
        if (err) {
          return cb(err);
        }

        fs.rmdir(directory, function (err) {
          cb(err && err.code !== 'ENOTEMPTY' ? err : undefined);
        });
      });
    });
  });
}

module.exports = destroy
