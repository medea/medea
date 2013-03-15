var assert = require('assert');
var msgpack = require('msgpack-js');
var Medea = require('./medea');

var medea = new Medea();

medea.open(function() {
  console.log('OPENED');
  var packed = msgpack.encode({ job: true, cow: 3, '?': new Buffer(0x32) });
  console.log('packed:',packed);
  medea.put('hello', packed, function() {
    medea.get('hello', function(val) {
      console.log('rerieved:', val);
      var decoded = msgpack.decode(val);
      console.log('RECEIVED:', decoded);
      medea.put('yo', 'dudex', function() {
        medea.get('yo', function(val2) {
          console.log('RECEIVED:', val2.toString());
          medea.remove('hello', function() {
            medea.get('hello', function(val3) {
              assert(!val3);
              console.log('REMOVED: hello');
              medea.close(function() {
                console.log('CLOSED');
              });
            });
          });
        });
      });
    });
  });
});
