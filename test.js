var assert = require('assert');
var msgpack = require('msgpack-js');
var Medea = require('./medea');

var medea = new Medea();

medea.open(function() {
  console.log('OPENED');
  medea.put('hello', msgpack.encode({ job: true, cow: 3, '?': new Buffer(0x32) }), function() {
    medea.get('hello', function(val) {
      console.log('RECEIVED:', msgpack.decode(val));
      medea.put('yo', ['a',1,'2'], function() {
        medea.get('yo', function(val2) {
          console.log('RECEIVED:', val2);
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
