var assert = require('assert');
var fs = require('fs');

var rimraf = require('rimraf');

var medea = require('../');

var directory = __dirname + '/tmp/medea_destroy_test';

describe('Medea#destroy', function() {
  before(function(done) {
    require('rimraf')(directory, function () {
      db = medea({ maxFileSize: 1024*1024 });
      db.open(directory, function(err) {
        done();
      });
    })
  });

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

    it('removes the folder', function (done) {
      medea.destroy(directory, function () {
        assert.equal(fs.existsSync(directory), false);
        done();
      });
    });
  });
});
