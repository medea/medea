var MappedItem = function() {
  this.recordKey = null;
  this.key = null;
  this.value = null;
};

var MapReduce = module.exports = function(db, options) {
  options = options || {};

  this.db = db;
  this.map = options.map;
  this.reduce = options.reduce;
  this.group = options.hasOwnProperty('group') ? options.group : false;
  this.startKey = options.startKey;
  this.endKey = options.endKey;
  this.mapped = [];
};

MapReduce.prototype.run = function(cb) {
  var self = this;

  self.db.listKeys(function(err, keys) {
    var len = keys.length;

    var acc;

    var iterator = function(keys, i, len, cb1) {
      if (i < len) {
        self.db.get(keys[i], function(err, val) {
          self.map(keys[i], val, self._mapper(keys[i]));
          iterator(keys, i+1, len, cb1);
        });
      } else {
        if (!self.group) {
          var remapped = {};
          self.mapped.forEach(function(item) {
            if (!remapped[item.key]) {
              remapped[item.key] = [];
            }

            remapped[item.key].push(item.value);
          });

          Object.keys(remapped).forEach(function(key) {
            if ((!self.startKey || (item.key >= self.startKey)) &&
                (!self.endKey || item.key <= self.endKey)) {
              acc = self.reduce(key, remapped[key]);
            }
          });
        } else {
          self.mapped.forEach(function(item) {
            if ((!self.startKey || (item.key >= self.startKey)) &&
                (!self.endKey || item.key <= self.endKey)) {
              acc = self.reduce(item.key, item.value);
            }
          });
        }

        cb1(acc);
      }
    };

    iterator(keys, 0, len, cb);
  });
};

MapReduce.prototype._mapper = function(recordKey) {
  var self = this;

  return function(key, val) {
    var item = new MappedItem();
    item.recordKey = recordKey;
    item.key = key;
    item.value = val;
    self.mapped.push(item);
  }
};
