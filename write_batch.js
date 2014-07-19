var DataEntry = require('./data_entry');
var timestamp = require('monotonic-timestamp');

var WriteBatch = module.exports = function() {
  this.keys = [];
  this.entries = [];
};

WriteBatch.prototype.put = function(key, value) {
  this._add(key, value);
};

WriteBatch.prototype.remove = function(key) {
  this._add(key, new Buffer('medea_tombstone'));
};

WriteBatch.prototype._add = function(key, value) {
  this.keys.push(key);
  this.entries.push(DataEntry.fromKeyValuePair(key, value));
};
