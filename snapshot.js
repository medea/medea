var Snapshot = module.exports = function (db) {
  var that = this;

  this.keydir = {};
  this.closed = false;
  this.fileIds = [];
  this.db = db

  Object.keys(db.keydir).forEach(function (key) {
    that.keydir[key] = db.keydir[key];
    if (that.fileIds.indexOf(db.keydir[key].fileId) === -1)
        that.fileIds.push(db.keydir[key].fileId)
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