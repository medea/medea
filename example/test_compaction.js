var Medea = require('../');

var db = new Medea();

db.open(function() {
  db.compact(function() {
    db.close(function() {
      console.log('done');
    });
  });
});
