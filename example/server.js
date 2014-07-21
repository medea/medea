var argo = require('argo');
var medea = require('../');

var db = medea();

var server = argo()
  .get('/db', function(handle) {
    handle('request', function(env, next) {
      var key = env.request.url.substr('/db/'.length);

      if (!key.length) {
        env.response.statusCode = 404;
        next(env);
        return;
      }
      
      db.get(key, function(err, val) {
        if (err) {
          console.log(err);
        }
        if (!val) {
          env.response.statusCode = 404;
          next(env);
          return;
        }
        env.response.statusCode = 200;
        env.response.body = val.toString();
        next(env);
      });
    });
  })
  .put('/db', function(handle) {
    handle('request', function(env, next) {
      var key = env.request.url.substr('/db/'.length);

      if (key === 'compact') {
        db.compact();
        env.response.statusCode = 202;
        next(env);
      }

      if (!key.length) {
        env.response.statusCode = 404;
        next(env);
        return;
      }

      env.request.getBody(function(err, body) {
        db.put(key, body, function() {
          env.response.statusCode = 201;
          next(env);
        });
      });
    });
  })
  .del('/db', function(handle) {
    handle('request', function(env, next) {
      var key = env.request.url.substr('/db/'.length);

      if (!key.length) {
        env.response.statusCode = 404;
        next(env);
        return;
      }

      db.remove(key, function() {
        env.statusCode = 200;
        next(env);
      });

    });
  });

db.open(function() {
  var port = process.env.PORT || 3000;
  server.listen(port);
  console.log('Listening on http://localhost:' + port);
  console.log('keydir length:', Object.keys(db.keydir).length);
});

['SIGTERM','SIGINT'].forEach(function(ev) {
  process.on(ev, function() {
    db.close(function() { console.log('closed db'); process.exit();});
  });
});
