var medea = require('../');
var db = medea();

db.open(function() {
  var map = function(key, value, emit) {
    emit('value', Number(value.toString().replace('valz', '')));
  };

  var reduce = function(key, values) {
    var sorted = values.sort(function(a, b) {
      if (a < b) return 1;
      if (a > b) return - 1;
      return 0;
    });

    return sorted;
  };

  var options = {
    map: map,
    reduce: reduce
  };

  db.mapReduce(options, function(values) {
    console.log(values[0]);
  });
});
