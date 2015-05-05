var Map = global.Map || require('es6-map');


var Snapshot = module.exports = function (db) {
  var that = this;

  this.keydir = new Map();
  this.closed = false;
  this.fileIds = [];
  this.db = db

  db.keydir.forEach(function (value, key) {
    that.keydir.set(key, db.keydir.get(key));
    if (that.fileIds.indexOf(db.keydir.get(key).fileId) === -1)
        that.fileIds.push(db.keydir.get(key).fileId)
  });
  this.fileIds.forEach(function (fileId) {
    db.fileReferences[fileId] = (db.fileReferences[fileId] || 0) + 1;
  })
};

Snapshot.prototype.close = function () {
  if (this.closed)
    throw new Error('Snapshot is already closed');

  var that = this

  this.closed = true;
  this.keydir = null;
  this.fileIds.forEach(function (fileId) {
    that.db.fileReferences[fileId]--;
    if (that.db.fileReferences[fileId] === 0)
      delete that.db.fileReferences[fileId]
  })
}
