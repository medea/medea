var constants = require('./constants');
var headerOffsets = constants.headerOffsets;
var sizes = constants.sizes;

var DataEntry = module.exports = function(buffer) {
  this.buffer = buffer;
  this.key = null;
  this.value = null;
  this.timestamp = null;
  this.keySize = null;
  this.valueSize = null;
  this.crc = null;
};

DataEntry.fromBuffer = function(buf) {
  var entry = new DataEntry(buf);

  entry.crc = buf.slice(headerOffsets.crc, sizes.crc);
  entry.timestamp = buf.readDoubleBE(headerOffsets.timestamp);
  entry.keySize = buf.readUInt16BE(headerOffsets.keysize);
  entry.valueSize = buf.readUInt32BE(headerOffsets.valsize);

  entry.key = buf.slice(sizes.header, sizes.header + entry.keySize);
  entry.value = buf.slice(sizes.header + entry.keySize, sizes.header + entry.keySize + entry.valueSize);

  return entry;
};

DataEntry.fromKeyValuePair = function(key, value) {
  if (!(key instanceof Buffer)) {
    key = new Buffer(key.toString());
  }

  if (!(value instanceof Buffer) && typeof value !== 'string') {
    value = new Buffer(value.toString());
  }

  var entry = new DataEntry();
  entry.key = key;
  entry.value = value;

  return entry;
};
