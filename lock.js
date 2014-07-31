var fs = require('fs');
var constants = require('constants');

var Lock = module.exports = function() {
  this.fd = null;
  this.type = null;
  this.filename = null;
};

Lock.prototype.isWriteFile = function() {
  return this.type.toLowerCase() === 'write';
};

Lock.acquire = function(filename, isWriteLock, cb) {
  var writeLockFlags = constants.O_CREAT | constants.O_EXCL | constants.O_RDWR | constants.O_SYNC;

  var flags = isWriteLock ? writeLockFlags : constants.O_RDONLY;

  fs.open(filename, flags, 0600, function(err, fd) {
    if (err) {
      cb(err);
      return;
    }

    var writeLock = new Lock();
    writeLock.filename = filename;
    writeLock.type = 'write';
    writeLock.fd = fd;

    cb(null, writeLock);
  });
};

Lock.readActiveFile = function(dirname, type, cb) {
  var filename = dirname + '/medea.' + type + '.lock';
  Lock.acquire(filename, false, function(err, lock) {
    if (err) {
      cb(err);
      return;
    }

    var bufs = [];
    var len = 0;
    var ondata = function(chunk) {
      bufs.push(chunk);
      len += chunk.length;
    };

    var onend = function() {
      if (!bufs || !bufs.length) {
        cb(null, '');
        return;
      };

      var text = Buffer.concat(bufs, len).toString();
      var arr = text.replace('\n', '').split(' ');

      // release lock?
      if (arr.length && arr[1]) {
        cb(null, arr[1]);
      } else {
        cb(null, '');
      }
    };

    var stream = fs.createReadStream(filename, { fd: lock.fd });
    stream.on('data', ondata);
    stream.on('end', onend);
    stream.on('error', function(err) {
      cb(err);
    });
  });
};

Lock.prototype.writeActiveFile = function(dirname, file, cb) {
  var filename = file ? file.filename : '';
  var lockfile = dirname + '/medea.write.lock';
  var stream = fs.createWriteStream(lockfile, { fd: this.fd, start: 0 });
  stream.write(process.pid + ' ' + (filename || '') + '\n', function(err) {
    if (err) {
      console.log('Error on writing active file to write lock.', err);
    }
    cb(null);
  });
};
