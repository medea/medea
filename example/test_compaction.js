var medea = require('../');

var db = medea();

db.open(function() {
  db.compact(function() {
    db.close(function() {
      console.log('done');
    });
  });
});
