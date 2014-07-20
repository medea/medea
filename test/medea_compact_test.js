var assert = require('assert');
var fs = require('fs');
var Medea = require('../');

var directory = __dirname + '/tmp/medea_test';
var db;
var setup = function (done) {
  require('rimraf')(directory, function () {
    db = new Medea({ maxFileSize: 512 });
    db.open(directory, done);
  });
}
var shutdown = function (done) {
  db.close(done);
}

describe('Medea#compact', function() {

  describe('Multiple time overwriting same key', function () {
    before(setup);

    it('successfully owerwites same key', function (done) {
      var test = function (index) {
        if (index === 50)
          return done();

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
      test(0);
    });

    it('obeys maxFileSize', function () {
      fs.readdirSync(directory).forEach(function (filename) {
        filename = directory + '/' + filename;

        var stat = fs.statSync(filename)
        assert(stat.size < db.maxFileSize);
      });
    });

    after(shutdown);
  });

  describe('Multiple time writing different keys', function () {
    before(setup);

    it('successfully writing different key', function (done) {
      var test = function (index) {
        if (index === 50)
          return done();

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
      test(0);
    });

    it('obeys maxFileSize', function () {
      fs.readdirSync(directory).forEach(function (filename) {
        filename = directory + '/' + filename;

        var stat = fs.statSync(filename)
        assert(stat.size < db.maxFileSize);
      });
    });

    after(shutdown);
  });

  describe('Write large amount of data', function () {
    before(setup);
    var max = 100,
      bufferLength = 100;

    before(function (done) {
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

        var stat = fs.statSync(filename)
        assert(stat.size < db.maxFileSize);
      });
    });

    after(shutdown);
  })
});
