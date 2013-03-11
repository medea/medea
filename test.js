var assert = require('assert');
var Medea = require('./medea');

var medea = new Medea();

medea.open(function() {
  console.log('OPENED');
  medea.put('hello', 'world!', function() {
    medea.get('hello', function(val) {
      console.log('RECEIVED:', val.toString());
      medea.put('yo', 'dudez!', function() {
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
