var assert = require('assert');
var fs = require('fs');
var path = require('path');
var medea = require('../');
var rimraf = require('rimraf');

var root = __dirname + '/tmp/medea_compact_test';
var setup = function (done) {
  require('rimraf')(directory, done);
};

describe('Medea#compact', function() {
  // disable timeout
  this.timeout(0);

  describe('Multiple time overwriting same key', function () {
    it('successfully overwites same key', function (done) {
      var directory = root + 'overwrites_same_key';
      var db = medea({ maxFileSize: 512 });
      var test = function (index) {
        if (index === 50) {
          db.close(function() {
            fs.readdirSync(directory).forEach(function (filename) {
              filename = directory + '/' + filename;

              try {
                var stat = fs.statSync(filename)
                assert(stat.size < db.maxFileSize);
              } catch(e) {
                if (e.code !== 'EPERM') { // issues/51
                  throw e;
                }
              }
            });

            done();
          });

          return;
        }

        var buffer1 = new Buffer(50),
          buffer2 = new Buffer(50),
          buffer3 = new Buffer(50);

        buffer1.fill(1);
        buffer2.fill(2);
        buffer3.fill(3);

        db.put('foo1', buffer1, function (err) {
          if (err) return done(err);
          db.put('foo1', buffer2, function (err) {
            if (err) return done(err);
            db.put('foo1', buffer3, function (err) {
              if (err) return done(err);
              db.compact(function (err) {
                if (err) return done(err)
                db.compact(function (err) {
                  if (err) return done(err)

                  db.get('foo1', function (err, value) {
                    assert.deepEqual(value, buffer3);
                    test(++index);
                  })
                });
              });
            });
          });
        });
      }
      db.open(directory, function() {
        test(0);
      });
    });
  });

  describe('Multiple time writing different keys', function () {

    it('successfully writing different key', function (done) {
      var db = medea({ maxFileSize: 512 });
      var directory = root + 'writes_different_key';
      var test = function (index) {
        if (index === 50) {
          db.close(function() {
            fs.readdirSync(directory).forEach(function (filename) {
              filename = directory + '/' + filename;

              try {
                var stat = fs.statSync(filename)
                assert(stat.size < db.maxFileSize);
              } catch(e) {
                if (e.code !== 'EPERM') { // issues/51
                  throw e;
                }
              }
            });

            done();
          });

          return;
        }


        var buffer1 = new Buffer(50),
          buffer2 = new Buffer(50),
          buffer3 = new Buffer(50);

        buffer1.fill(1);
        buffer2.fill(2);
        buffer3.fill(3);

        db.put('foo1', buffer1, function (err) {
          if (err) return done(err);
          db.put('foo2', buffer2, function (err) {
            if (err) return done(err);
            db.put('foo3', buffer3, function (err) {
              if (err) return done(err);
              db.compact(function (err) {
                if (err) return done(err)
                db.compact(function (err) {
                  if (err) return done(err)
                  db.get('foo1', function (err, value) {
                    assert.deepEqual(value, buffer1);
                    db.get('foo2', function (err, value) {
                      assert.deepEqual(value, buffer2);
                      db.get('foo3', function (err, value) {
                        assert.deepEqual(value, buffer3);

                        test(++index);
                      });
                    });
                  })
                });
              });
            });
          });
        });
      }
      db.open(directory, function() {
        test(0);
      });
    });
  });

  describe('Write large amount of data', function () {
    var max = 100,
      bufferLength = 100;

    var db = medea({ maxFileSize: 512 });
    var directory = root + 'large_amounts';
    before(function (done) {
      rimraf(directory, function() {
        db.open(directory, function() {
          var put = function(index) {
            if (index === max) {
              return done();
            }

            var buffer = new Buffer(bufferLength);

            buffer.fill(index);

            db.put(buffer, buffer, function(err) {
              if (err) return done(err);
              put(++index);
            });
          }

          put(0);
        });
      });
    });

    it('successfully compacts', function (done) {
      db.compact(function (err) {
        if (err) return done(err);
        db.compact(function (err) {
          if (err) return done(err);
          done()
        });
      });
    });

    it('successfully saves data', function (done) {
      var get = function (index) {
        if (index === max) {
          return done();
        }
        var buffer = new Buffer(bufferLength);
        buffer.fill(index);

        db.get(buffer, function (err, value) {
          if (err) return done(err);
          assert.deepEqual(value, buffer);
          get(++index);
        });
      }
      get(0);
    });

    it('obeys maxFileSize', function () {
      fs.readdirSync(directory).forEach(function (filename) {
        filename = directory + '/' + filename;

        try {
          var stat = fs.statSync(filename)
          assert(stat.size < db.maxFileSize);
        } catch(e) {
          if (e.code !== 'EPERM') { // issues/51
            throw e;
          }
        }
      });
    });
  });

  describe('write many keys', function () {
    var db = medea({ maxFileSize: 1024 * 1024 });
    var directory = root + 'many_keys';

    var max = 25000;

    before(function (done) {
      require('rimraf')(directory, function () {
        db.open(directory, function() {
          var put = function(index) {
            if (index === max) {
              return done();
            }

            db.put(index.toString(), index.toString(), function(err) {
              if (err) return done(err);
              put(++index);
            });
          }

          put(0);
        });
      });
    });

    it('successfully compacts', function (done) {
      db.compact(function (err) {
        if (err) return done(err);
        db.compact(function (err) {
          if (err) return done(err);
          done()
        });
      });
    });

    it('successfully saves data', function (done) {
      var get = function (index) {
        if (index === max) {
          return done();
        }

        db.get(index.toString(), function (err, value) {
          if (err) return done(err);
          assert.deepEqual(value.toString(), index.toString());
          get(++index);
        });
      }
      get(0);
    });

    it('obeys maxFileSize', function () {
      fs.readdirSync(directory).forEach(function (filename) {
        filename = directory + '/' + filename;

        var stat = fs.statSync(filename)
        assert(stat.size < db.maxFileSize);
      });
    });
  })

  describe('handle missing hint files', function () {
    it('should pass', function(done) {
      var directory = root;
      var db1 = medea({});
      require('rimraf')(directory, function() {
        db1.open(directory, function(err) {
          assert(!err);
          db1.put('hello', 'world', function(err) {
            assert(!err);
            db1.close(function(err) {
              assert(!err);
              rimraf(directory + '/1.medea.hint', function() {
                var db2 = medea({});
                db2.open(directory, function(err) {
                  assert(!err);
                  
                  db2.compact(function(err) {
                    if (err) {
                      throw err;
                    }
                    done();
                  })
                });
              });
            });
          });
        });
      });
    });
  });

  describe('cleanup dangling hint files', function () {
    it('should pass', function(done) {
      var directory = root;
      var db1 = medea({});
      require('rimraf')(directory, function() {
        db1.open(directory, function(err) {
          assert(!err);
          db1.put('hello', 'world', function(err) {
            assert(!err);
            db1.close(function(err) {
              var db2 = medea({});
              db2.open(directory, function(err) {
                assert(!err);


                if (process.platform !== 'win32') {
                  // prevent windows from erroring due to in-progress I/O completion state.
                  var fds = [];
                  for (var i = 10; i < 15; i++) {
                    var fd = fs.openSync(path.join(directory, i + '.medea.hint'), 'w');
                    fds.push(fd);
                  }

                  fds.forEach(function(fd) {
                    fs.closeSync(fd);
                  });

                  var files = fs.readdirSync(directory);
                  assert.equal(files.length, 10);
                }

                db2.compact(function(err) {
                  assert(!err);

                  var files = fs.readdirSync(directory);

                  var hintFileNumbers = files.filter(function(file) {
                    return file.indexOf('hint') > -1;
                  }).map(function(file) {
                    return file[0];
                  });

                  hintFileNumbers.forEach(function(number) {
                    assert(files.indexOf(number + '.medea.data') > -1);
                  });

                  done();
                });
              });
            });
          });
        });
      });
    });
  });
});
