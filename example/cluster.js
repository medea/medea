var cluster = require('cluster');
var argo = require('argo-server');
var numCPUs = require('os').cpus().length;
var Medea = require('../');

var medea = new Medea();

if (cluster.isMaster) {
  // Run medea.setupMaster() in the master process.
  medea.setupMaster();

  for (var i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  ['SIGTERM','SIGINT'].forEach(function(ev) {
    process.on(ev, function() {
      medea.close();
    });
  });
} else {
  var server = argo()
    .get('/', function(handle) {
      handle('request', function(env, next) {
        medea.get(env.request.url, function(err, cached) {
          if (cached) {
            env.response.body = cached.toString();
            next(env);
          } else {
            env.response.body = 'Hello World!';
            medea.put(env.request.url, env.response.body, function(err) {
              next(env);
            });
          }
        });
      });
    });

  medea.open(function() {
    server.listen(3000);
    console.log('listening on port 3000');
  });
}
