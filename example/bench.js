var str = {
  large : new Buffer('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890'),
  medium : new Buffer('1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890'),
  small : new Buffer('12345678901234567890')
}

var medea = require('../');
var db = medea();

var i = 0;
var MAX_RUNS = 100000;

require('rimraf')(process.cwd() + '/db', function () {
  db.open(function() {
    run('set small', setSmall, function() {
      run('set medium', setMedium, function() {
        run('set large', setLarge, function() {
          run('get large', getLarge, function() {
            run('get medium', getMedium, function() {
              run('get small', getSmall, function() {
                db.close()
              });
            });
          });
        });
      });
    });
  });
});

var run = function(name, bench, next) {
  console.log(name + ' >>');
  var start = Date.now();
  var counter = 1;
  for (var j = 0; j < MAX_RUNS; j++) {
    bench(function() {
      counter++;
      if (counter === MAX_RUNS) {
        var end = (Date.now() - start) / 1000; // seconds
        var opsPerSecond = MAX_RUNS / end;
        var fixed = opsPerSecond * 100; // to get to the hundredths; 50.321 * 100 = 5032.1
        var speed = Math.round(fixed) / 100; // ; round(5032.1) = 5032; 5032 / 100 = 50.32
        console.log('\ttime:', speed + ' ops/s');
        next();
      }
    });
  }
};

var setSmall = function(done) {
  db.put((i++).toString(), str.small, done);
};

var setMedium = function(done) {
  db.put((i++).toString(), str.medium, done);
};

var setLarge = function(done) {
  db.put((i++).toString(), str.large, done);
};

var getLarge = function(done) {
  db.get((--i).toString(), done);
}

var getMedium = function(done) {
  db.get((--i).toString(), done);
};

var getSmall = function(done) {
  db.get((--i).toString(), done);
};
  //bench('set small', function(done) {  db.put((i++).toString(), str.small, done); });
  //bench('set medium', function(done) { db.put((i++).toString(), str.medium, done); });
  //bench('set large', function(done) { db.put((i++).toString(), str.large, done); });
  //bench('get large', function(done) { db.get((--i).toString(), done); });
  //bench('get medium', function(done) { db.get((--i).toString(), done); });
  //bench('get small', function(done) { db.get((--i).toString(), done); });
