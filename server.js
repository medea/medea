var argo = require('argo-server');
var Medea = require('./medea');

var options = {
  maxFileSize: 1024,
};

var medea = new Medea(options);

var server = argo()
  .get('/bitcask', function(handle) {
    handle('request', function(env, next) {
      var key = env.request.url.substr('/bitcask/'.length);

      if (!key.length) {
        env.response.statusCode = 404;
        next(env);
        return;
      }
      
      medea.get(key, function(val) {
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
  .put('/bitcask', function(handle) {
    handle('request', function(env, next) {
      var key = env.request.url.substr('/bitcask/'.length);

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
  .del('/bitcask', function(handle) {
    handle('request', function(env, next) {
      var key = env.request.url.substr('/bitcask/'.length);

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
