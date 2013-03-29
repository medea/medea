var cluster = require('cluster');
var util = require('util');

var MedeaCluster = function() {
  var Medea = MedeaCluster.super_;
  Medea.apply(this, arguments);
  this.opened = false;
  this._open = Medea.prototype.open.bind(this);
  this._get = Medea.prototype.get.bind(this);
  this._put = Medea.prototype.put.bind(this);
  this._remove = Medea.prototype.remove.bind(this);
  this._listKeys = Medea.prototype.listKeys.bind(this);
  this._sync = Medea.prototype.sync.bind(this);
  this._close = Medea.prototype.close.bind(this);
};

function enhance(MedeaCluster) {
  MedeaCluster.prototype.setupMaster = function() {
    var that = this;
    cluster.on('fork', function(worker) {
      that._setupWorker(worker);
    });

    Object.keys(cluster.workers).forEach(function(id) {
      that._setupWorker(cluster.workers[id]);
    });
  };

  MedeaCluster.prototype._setupWorker = function(worker) {
    var that = this;
    worker.on('message', function(data) {
      if (data.command === 'open') {
        that._open(data.dirname, data.options, function(err) {
          var msg = { event: 'opened', err: err };
          worker.send(msg);
        });
      } else if (data.command === 'get') {
        that._get(data.key, function(err, val) {
          var msg = { event: 'retrieved', err: err };

          if (val) {
            msg.value = val.toString('base64');
          }

          worker.send(msg);
        });
      } else if (data.command === 'put') {
        that._put(data.key, data.value, function(err) {
          var msg = { event: 'inserted', err: err };
          worker.send(msg);
        });
      } else if (data.command === 'remove') {
        that._remove(data.key, function(err) {
          var msg = { event: 'removed', err: err };
          worker.send(msg);
        });
      } else if (data.command === 'listKeys') {
        that._listKeys(function(err, keys) {
          var msg = { event: 'keysListed', err: err, keys: keys };
          worker.send(msg);
        });
      } else if (data.command === 'synchronize') {
        that._sync(function(err) {
          var msg = { event: 'synchronized', err: err };
          worker.send(msg);
        });
      }
    });
  };

  MedeaCluster.prototype.open = function(dir, options, cb) {
    if (typeof options === 'function') {
      cb = options;
      options = {};
    }

    if (typeof dir === 'function') {
      options = {};
      cb = dir;
      dir = this.dirname || __dirname + '/medea';
    }

    if (cluster.isWorker) {
      process.once('message', function(data) {
        if (data.event === 'opened') {
          cb(data.err);
        }
      });
      process.send({ command: 'open', dirname: dir, options: options });
      return;
    }

    if (this.opened) {
      cb();
      return;
    }

    this.opened = true;

    this._open(dir, options, cb);
  };

  MedeaCluster.prototype.get = function(key, cb) {
    if (cluster.isWorker) {
      process.once('message', function(data) {
        if (data.event === 'retrieved') {
          var val;
          if (data.value) {
            val = new Buffer(data.value, 'base64');
          }
          cb(data.err, val);
        }
      });
      process.send({ command: 'get', key: key });
      return;
    }

    this._get(key, cb);
  };

  MedeaCluster.prototype.put = function(k, v, cb) {
    if (cluster.isWorker) {
      process.once('message', function(data) {
        if (data.event === 'inserted') {
          cb(data.err);
        }
      });
      process.send({ command: 'put', key: k, value: v });
      return;
    }

    this._put(k, v, cb);
  };

  MedeaCluster.prototype.remove = function(key, cb) {
    if (cluster.isWorker) {
      process.once('message', function(data) {
        if (data.event === 'removed') {
          cb(data.err);
        }
      });
      process.send({ command: 'remove', key: key });
      return;
    }

    this._remove(key, cb);
  };

  MedeaCluster.prototype.listKeys = function(cb) {
    if (cluster.isWorker) {
      process.once('message', function(data) {
        if (data.event === 'keysListed') {
          cb(data.err, data.keys);
        }
      });
      process.send({ command: 'listKeys' });
      return;
    }

    this._listKeys(cb);
  };

  MedeaCluster.prototype.sync = function(cb) {
    if (cluster.isWorker) {
      process.once('message', function(data) {
        if (data.event === 'synchronized') {
          cb(data.err);
        }
      });
      process.send({ command: 'synchronize' });
      return;
    }

    this._sync(cb);
  };

  MedeaCluster.prototype.close = function(cb) {
    if (cluster.isWorker) {
      cb();
      return;
    }

    this._close(cb);
  };

  return MedeaCluster;
}

module.exports = function(Medea) {
  util.inherits(MedeaCluster, Medea);
  return enhance(MedeaCluster);
};
