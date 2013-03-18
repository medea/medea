var fs = require('fs');
var fsflags = require('./constants').fsflags;

var Lock = module.exports = function() {
  this.fd = null;
  this.type = null;
  this.filename = null;
};

Lock.prototype.isWriteFile = function() {
  return this.type.toLowerCase() === 'write';
};

Lock.acquire = function(filename, isWriteLock, cb) {
  var writeLockFlags = fsflags.O_CREAT | fsflags.O_EXCL | fsflags.O_RDWR | fsflags.O_SYNC;

  var flags = isWriteLock ? writeLockFlags : fsflags.O_RDONLY;

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

    var onend = function(lockFd) {
      return function() {
        if (!bufs || !bufs.length) {
          cb(null, '');
          return;
        };
        var all = new Buffer(len);
        var offset = 0;
        bufs.forEach(function(buf) {
          buf.copy(all, 0, offset, buf.length);
          offset += buf.length;
        });

        var text = all.toString();

        var arr = text.replace('\n', '').split(' ');

        // release lock?
        if (arr.length && arr[1]) {
          cb(null, arr[1]);
        } else {
          cb(null, '');
        }
      };
    };

    var stream = fs.createReadStream(filename, { fd: lock.fd });
    stream.on('data', ondata);
    stream.on('end', onend(lock.fd));
    stream.on('error', function(err) {
      cb(err);
    });

    stream.resume();
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

Lock.prototype.writeActiveFileSync = function(dirname, file) {
  var filename = file ? file.filename : '';
  var lockfile = dirname + '/medea.write.lock';

  var buf = new Buffer(process.pid + ' ' + (filename || '') + '\n');
  fs.writeSync(this.fd, buf, 0, buf.length, 0);
};
