var assert = require('assert');
var Medea = require('./medea');

var medea = new Medea();

medea.open(function() {
  console.log('OPENED');
  medea.put('hello', { job: true, cow: 3, '?': new Buffer(0x32) }, function() {
    medea.get('hello', function(val) {
      console.log('RECEIVED:', val);
      medea.put('yo', 'dudez!', function() {
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
