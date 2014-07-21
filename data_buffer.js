var crc32 = require('buffer-crc32');
var constants = require('./constants');
var headerOffsets = constants.headerOffsets;
var sizes = constants.sizes;

exports.fromKeyValuePair = function(key, value, ts) {
  if (!(key instanceof Buffer)) {
    key = new Buffer(key.toString());
  }

  if (!(value instanceof Buffer) && typeof value !== 'string') {
    value = new Buffer(value.toString());
  }

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

  return lineBuffer;
};

