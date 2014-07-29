var fs = require('fs');
var constants = require('./constants');
var sizes = constants.sizes;

exports.createWriteStream = function (dir, cb) {
  var stream = fs.createWriteStream(dir);
  var onerror = function (err) {
    stream.removeListener('open', onopen);
    cb(err);
  };
  var onopen = function () {
    stream.removeListener('error', onerror);
    cb(null, stream);
  };

  stream.once('error', onerror);
  stream.once('open', onopen);
}

exports.ensureDir = function(dir, cb) {
  fs.stat(dir, function(err, stat) {
    if (!stat) {
      fs.mkdir(dir, function(err) {
        if (err) {
          cb(err);
          return;
        }
        if (cb) cb();
      });
    } else {
      if (cb) cb();
    }
  });
};

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

var dataFileTstampsSync = exports.dataFileTstampsSync = function(dirname) {
  var files = fs.readdirSync(dirname);
  var tstamps = [];

  files.forEach(function(file) {
    var match = file.match(/^([0-9]+).medea.data/);
    if (match && match.length && match[1]) {
      tstamps.push(Number(match[1]));
    }
  }); 
    
  return tstamps;
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

exports.mostRecentTstampSync = function(dirname) {
  var stamps = dataFileTstampsSync(dirname);
  if (stamps.length) {
    return stamps.sort(function(a, b) {
      if (a > b) return -1;
      if (a < b) return 1;
      return 0;
    })[0];
  } else {
    return 0;
  }
};

exports.listDataFiles = function(dirname, writeFile, mergeFile, cb) {
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

    [writeFile, mergeFile].forEach(function(f) {
      if (f) {
        var n = f.replace('.medea.data', '');
        var index = sorted.indexOf(n);
        if (index !== -1) {
          delete sorted[index];
        }
      }
    });

    var ret = sorted.map(function(t) {
      return dirname + '/' + t + '.medea.data';
    });

    cb(null, ret);
  });
};
