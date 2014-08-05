var assert = require('assert');
var crc32 = require('buffer-crc32');
var constants = require('../constants');
var DataFile = require('../data_file');
var DataFileParser = require('../data_file_parser');

var headerOffsets = constants.headerOffsets;
var sizes = constants.sizes;

var directory = __dirname + '/tmp/data_file_parser_test';
var file;

function createBuffer(k, v) {
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

  return lineBuffer;
}

function createStringBuffer(k, v) {
  return createBuffer(new Buffer(k), new Buffer(v));
}

function writeBuffers(bufs) {
  bufs.forEach(function(buf) {
    file.write(buf);
  });
}

describe('DataFileParser', function() {
  before(function(done) {
    require('rimraf').sync(directory);
    require('mkdirp').sync(directory);

    DataFile.create(directory, function(err, f) {
      file = f;
      done();
    });
  });

  describe('#parse', function() {
    it('parses data file entries', function(done) {
      var parser = new DataFileParser(file);
      var lineBuffer1 = createStringBuffer('hello', 'world');
      var lineBuffer2 = createStringBuffer('hola', 'mundo');

      writeBuffers([lineBuffer1, lineBuffer2]);

      var count = 0;
      parser.on('entry', function(entry) {
        count++;
        if (count === 1) {
          assert.equal(entry.key.toString(), 'hello');
          assert.equal(entry.value.toString(), 'world');
        } else if (count === 2) {
          assert.equal(entry.key.toString(), 'hola');
          assert.equal(entry.value.toString(), 'mundo');
          done();
        }
      });
      parser.parse();
    });

    it('parses large data files', function(done) {
      var key = new Buffer(500);
      key.fill('k');
      var val = new Buffer(500);
      val.fill('v');

      for (var i = 0; i < 4000; i++) {
        writeBuffers([createBuffer(key, val)]);
      }

      var parser = new DataFileParser(file);
      parser.parse();

      parser.on('end', function() {
        done();
      });
    });

    it('elevates fs errors', function(done) {
      f = { filename: directory + '/blah.txt' };
      var parser = new DataFileParser(f);
      parser.on('error', function(err) {
        assert(!!err);
        done();
      });

      parser.parse();
    });
  });

  after(function(done) {
    file.closeForWriting(done);
  });
});
