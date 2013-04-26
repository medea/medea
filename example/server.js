var argo = require('argo');
var Medea = require('../');

var medea = new Medea();

var server = argo()
  .get('/medea', function(handle) {
    handle('request', function(env, next) {
      var key = env.request.url.substr('/medea/'.length);

      if (!key.length) {
        env.response.statusCode = 404;
        next(env);
        return;
      }
      
      medea.get(key, function(err, val) {
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
  .put('/medea', function(handle) {
    handle('request', function(env, next) {
      var key = env.request.url.substr('/medea/'.length);

      if (key === 'compact') {
        medea.compact();
        env.response.statusCode = 202;
        next(env);
      }

      if (!key.length) {
        env.response.statusCode = 404;
        next(env);
        return;
      }

      env.request.getBody(function(err, body) {
        medea.put(key, body, function() {
          env.response.statusCode = 201;
          next(env);
        });
      });
    });
  })
  .del('/medea', function(handle) {
    handle('request', function(env, next) {
      var key = env.request.url.substr('/medea/'.length);

      if (!key.length) {
        env.response.statusCode = 404;
        next(env);
        return;
      }

      medea.remove(key, function() {
        env.statusCode = 200;
        next(env);
      });

    });
  });

medea.open(function() {
  var port = process.env.PORT || 3000;
  server.listen(port);
  console.log('Listening on http://localhost:' + port);
  console.log('keydir length:', Object.keys(medea.keydir).length);
});

['SIGTERM','SIGINT'].forEach(function(ev) {
  process.on(ev, function() {
    medea.close(function() { console.log('closed medea'); process.exit();});
  });
});
