var DataEntry = require('./data_entry');

var WriteBatch = module.exports = function() {
  this.buffers = [];
  this.keydir = {};
};

WriteBatch.prototype.put = function(key, value) {
  var dataEntry = DataEntry.fromKeyValuePair(key, value);
  this.buffers.push(dataEntry.buffer);
};

WriteBatch.prototype.remove = function(key) {
  var tombstone = new Buffer('medea_tombstone');
  var dataEntry = DataEntry.fromKeyValuePair(key, tombstone);
  this.buffers.push(dataEntry.buffer);
};
