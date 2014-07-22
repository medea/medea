var medea = require('../');

var db = medea();

db.open(function() {
  var start = Date.now();
  var batch = db.createBatch();

  for (var i = 0, len = 130000; i < len; i++) {
    batch.put('hello' + i, 'valz' + i);
  }

  db.write(batch, function() {
    console.log('time:', (Date.now() - start) / 1000, 's');
    db.close();
  });
});
