var assert = require('assert');
var crc32 = require('buffer-crc32');
var constants = require('../constants');
var DataEntry = require('../data_entry');
var headerOffsets = constants.headerOffsets;
var sizes = constants.sizes;

describe('DataEntry', function() {
  describe('.fromBuffer', function() {
    it('extracts properties from a Buffer', function() {
      var k = new Buffer('hello');
      var v = new Buffer('world');
      var ts = Date.now();

      var lineBuffer = new Buffer(sizes.header + k.length + v.length);
      var key = k;
      var value = v;

      lineBuffer.writeDoubleBE(ts, headerOffsets.timestamp);
      lineBuffer.writeUInt16BE(key.length, headerOffsets.keysize);
      lineBuffer.writeUInt32BE(value.length, headerOffsets.valsize);

      key.copy(lineBuffer, headerOffsets.valsize + sizes.valsize);
      value.copy(lineBuffer, headerOffsets.valsize + sizes.valsize + key.length);

      //using slice we are just referencing the originial buffer
      var crcBuf = crc32(lineBuffer.slice(headerOffsets.timestamp,  headerOffsets.valsize+ sizes.valsize));
      crcBuf = crc32(key, crcBuf);
      crcBuf = crc32(value, crcBuf);
      crcBuf.copy(lineBuffer)

      var entry = DataEntry.fromBuffer(lineBuffer);

      assert.equal(entry.crc.toString('hex'), crcBuf.toString('hex'));
      assert.equal(entry.timestamp, ts);
      assert.equal(entry.keySize, k.length);
      assert.equal(entry.valueSize, v.length);
      assert.equal(entry.key.toString(), k.toString());
      assert.equal(entry.value.toString(), v.toString());
    });
  });
});
