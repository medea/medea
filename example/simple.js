var assert = require('assert');
var msgpack = require('msgpack-js');
var medea = require('../');

var db = medea();

db.open(function() {
  console.log('OPENED');
  var packed = msgpack.encode({ job: true, cow: 3, '?': new Buffer(0x32) });
  db.put('hello', packed, function() {
    db.get('hello', function(err, val) {
      var decoded = msgpack.decode(val);
      console.log('RECEIVED:', decoded);
      db.put('yo', 'dudex', function() {
        db.get('yo', function(err, val2) {
          console.log('RECEIVED:', val2.toString());
          db.remove('hello', function() {
            db.get('hello', function(err, val3) {
              assert(!val3);
              console.log('REMOVED: hello');
              db.close(function() {
                console.log('CLOSED');
              });
            });
          });
        });
      });
    });
  });
});
