var medea = require('../');

var options = {
  //maxFileSize: 1024*1024
};

var db = medea(options);

var counter = 0;

db.open(function() {
  var start = Date.now();
  for (var i = 0, len = 130000; i < len; i++) {
    if (i === len - 1) {
      db.put('hello' + i, 'valz' + i, function() {
        console.log('time:', (Date.now() - start) / 1000, 's');
        db.close();
      });
    } else {
      db.put('hello' + i, 'valz' + i);
    }
  }
});
