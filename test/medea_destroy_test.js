var assert = require('assert');
var fs = require('fs');

var rimraf = require('rimraf');

var medea = require('../');

var directory = __dirname + '/tmp/medea_destroy_test';

describe('Medea#destroy', function() {
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

});
