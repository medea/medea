var Medea = require('./medea');
var medea = new Medea();

medea.open(function() {
  var map = function(key, value, emit) {
    emit('value', value.toString().replace('valz', ''));
  };

  var reduce = function(key, values) {
    var sorted = values.sort();
    return sorted[0];
  };

  medea.mapReduce(map, reduce, function(value) {
    console.log(value);
  });
});
