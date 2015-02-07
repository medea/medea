var fs = require('fs');
var path = require('path');
var constants = require('./constants');
var sizes = constants.sizes;

var dataFileTstamps = exports.dataFileTstamps = function(dirname, cb) {
  fs.readdir(dirname, function(err, files) {
    if (err) {
      cb(err);
      return;
    }

    var tstamps = [];
    
    files.forEach(function(file) {
      var match = file.match(/^([0-9]+).medea.data/);
      if (match && match.length && match[1]) {
        tstamps.push(Number(match[1]));
      }
    }); 

    cb(null, tstamps);
  });
};

exports.mostRecentTstamp = function(dirname, cb) {
  dataFileTstamps(dirname, function(err, stamps) {
    if (err) {
      cb(err);
      return;
    }

    if (stamps.length) {
      cb(null, stamps.sort(function(a, b) {
        if (a > b) return - 1;
        if (a < b) return 1;
        return 0;
      })[0]);
    } else {
      cb(null, 0);
    }
  });
};

exports.listDataFiles = function(dirname, cb) {
  dataFileTstamps(dirname, function(err, tstamps) {
    if (err) {
      cb(err);
      return;
    }

    var sorted = tstamps.sort(function(a, b) {
      if (a > b) return -1;
      if (a < b) return 1;
      return 0;
    });

    var ret = sorted.map(function(t) {
      return path.join(dirname, t + '.medea.data');
    });

    cb(null, ret);
  });
};
