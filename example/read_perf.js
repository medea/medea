var medea = require('../');
var db = medea({ readOnly: true });

var num = 130000;
var iterations = 1;

db.open(function() {
  var ids = new Array(num);
  for (var i = 0, len = ids.length; i < len; i++) {
    ids[i] = Math.floor(Math.random()*(num));
  }

  var start = Date.now();

  var counter = 0;
  var errors = [];
  for (var j = 0; j < iterations; j++) {
    for (var i = 0, len = ids.length; i < len; i++) {
      db.get('hello' + ids[i], function(err, val) {
        if (err) {
          errors.push(err);
          console.log(err);
        }
        counter++
        if (counter === num * iterations) {
          var end = Date.now() - start;

          var time = end / 1000;

          console.log('Completed in ' + time + 's');
          console.log('Read errors:', errors.length);
          console.log('Reads at ' + Math.round(((num * iterations)/ time) * 100) / 100 + ' values per second');
          db.close();
        }
      });
    }
  }

});
