var Medea = require('./medea');

var options = {
  maxFileSize: 1024*100
};

var medea = new Medea(options);

medea.open(function() {
  put(0, 1000);
});

var put = function(i, max) {
  if (i === max) {
    medea.close(function() { console.log('closed!'); });
    return;
  }

  medea.put('hello' + i, 'valz' + i, function() {
    put(i+1, max);
  });
}
