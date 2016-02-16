var assert = require('assert');
var fs = require('fs');
var path = require('path');

var rimraf = require('rimraf');

var medea = require('../');

var directory = __dirname + '/tmp/medea_destroy_test';

describe('Medea.destroy', function() {
  describe('initialized and closed db', function () {
    before(function (done) {
      rimraf(directory, function () {
        var db = medea();
        db.open(directory, function () {
          db.put('beep', 'boop', function () {
            db.close(done);
          });
        });
      });
    });

    it('deletes the folder', function (done) {
      medea.destroy(directory, function () {
        assert.equal(fs.existsSync(directory), false);
        done();
      });
    });
  });

  describe('folder with medea and some other files', function () {
    before(function (done) {
      rimraf(directory, function () {
        var db = medea();
        db.open(directory, function() {
          db.put('beep', 'boop', function () {
            db.close(function () {
              fs.writeFileSync(directory + '/beep.boop', 'beep boop');
              fs.writeFileSync(directory + '/hello.world', 'hello, world!');
              done();
            });
          });
        });
      });
    });

    it('deletes the medea files, but not the folder', function (done) {
      medea.destroy(directory, function (err) {
        if (err) return done(err);

        var filenames = fs.readdirSync(directory).sort();

        assert.deepEqual(filenames, [ 'beep.boop', 'hello.world' ]);
        done();
      });
    });
  });

  describe('folder with medea-files & lock-file', function () {
    before(function (done) {
      rimraf(directory, function () {
        var db = medea();
        db.open(directory, function() {
          db.put('beep', 'boop', function () {
            fs.writeFileSync(directory + '/medea.lock', '123')
            done()
          });
        });
      });
    });

    it('deletes the folder', function (done) {
      var dir = directory + 'deletes_folder';
      rimraf(dir, function () {
        var db = medea();
        db.open(dir, function() {
          db.put('beep', 'boop', function () {
            db.close(function() {
              fs.writeFileSync(path.join(directory, 'medea.lock'), '123')
              medea.destroy(dir, function (err) {
                fs.stat(dir, function(err, stat) {
                  assert(err);
                  done();
                });
              });
            });
          });
        });
      });
    });
  });

  describe('active medea instance', function () {
    it('errors', function (done) {
      rimraf(directory, function () {
        var db = medea();
        db.open(directory, function() {
          medea.destroy(directory, function (err) {
            assert(err instanceof Error, 'should error');
            done();
          });
        });
      });
    });
  });

});

describe('Medea#destroy', function () {
  describe('initialized and closed db', function () {
    it('deletes the folder', function (done) {
      var db;
      var dir = directory + 'initialize_and_close';
      rimraf(dir, function () {
        db = medea();
        db.open(dir, function () {
          db.put('beep', 'boop', function () {
            db.close(function() {
              db.destroy(function () {
                fs.stat(dir, function(err, stat) {
                  assert(err);
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
