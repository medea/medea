var DataEntry = require('./data_entry');

var WriteBatch = module.exports = function() {
  this.operations = [];
};

WriteBatch.prototype.put = function(key, value) {
  this.operations.push({
    type: 'put',
    entry: DataEntry.fromKeyValuePair(key, value)
  });
};

WriteBatch.prototype.remove = function(key) {
  var value = new Buffer('medea_tombstone');

  this.operations.push({
    type: 'remove',
    entry: DataEntry.fromKeyValuePair(key, value)
  });
};
