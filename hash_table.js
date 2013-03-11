// V8 implements objects as hash tables.
// Medea uses this implementation detail.

var HashTable = module.exports = function() {
  this._table = {};
};

HashTable.prototype.put = function(key, value) {
  this._table[key] = value;

  return value;
};

HashTable.prototype.get = function(key) {
  var value = this._table[key];

  return value;
};

HashTable.prototype.has = function(key) {
  return !!this._table[key];
};
