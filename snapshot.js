var Snapshot = module.exports = function (db) {
  var that = this;

  this.keydir = {};
  this.closed = false;

  Object.keys(db.keydir).forEach(function (key) {
    that.keydir[key] = db.keydir[key];
  });
};

Snapshot.prototype.close = function () {
  this.closed = true;
  this.keydir = null;
}