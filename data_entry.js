var crc32 = require('buffer-crc32');
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

  var ts = Date.now();

  /**
   * [crc][timestamp][keysz][valuesz][key][value]
   */
  var lineBuffer = new Buffer(sizes.header + key.length + value.length);

  lineBuffer.writeDoubleBE(ts, headerOffsets.timestamp);
  lineBuffer.writeUInt16BE(key.length, headerOffsets.keysize);
  lineBuffer.writeUInt32BE(value.length, headerOffsets.valsize);

  key.copy(lineBuffer, headerOffsets.valsize + sizes.valsize);
  if (typeof(value) === 'string')
    lineBuffer.write(value, headerOffsets.valsize + sizes.valsize + key.length);
  else
    value.copy(lineBuffer, headerOffsets.valsize + sizes.valsize + key.length);

  //using slice we are just referencing the originial buffer
  var crcBuf = crc32(lineBuffer.slice(headerOffsets.timestamp,  headerOffsets.valsize+ sizes.valsize));
  crcBuf = crc32(key, crcBuf);
  crcBuf = crc32(value, crcBuf);
  crcBuf.copy(lineBuffer);

  return new DataEntry.fromBuffer(lineBuffer);
};
