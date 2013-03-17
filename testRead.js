var Medea = require('./medea');
var medea = new Medea({ readOnly: true });

var num = 1000;

medea.open(function() {
  var ids = new Array(num);
  for (var i = 0, len = ids.length; i < len; i++) {
    ids[i] = Math.floor(Math.random()*(num+1));
  }

  var start = Date.now();

  var counter = 0;
  for (var i = 0, len = ids.length; i < len; i++) {
    medea.get('hello' + ids[i], function(val) {
      counter++
      if (counter === (len - 1)) {
        var end = Date.now() - start;

        var time = end / 1000;
        console.log('Completed in ' + time + 's');
        console.log('Reads at ' + Math.round((num / time) * 100) / 100 + ' values per second');
        medea.close();
      }
    });
  }

});
