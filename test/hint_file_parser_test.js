var assert = require('assert');
var constants = require('../constants');
var crc32 = require('buffer-crc32');
var DataFile = require('../data_file');
var HintFileParser = require('../hint_file_parser');
var KeyDirEntry = require('../keydir_entry');

var sizes = constants.sizes;
var headerOffsets = constants.headerOffsets;

var directory = __dirname + '/tmp/hint_file_parser_test';
var arr = [];
var files = [];
var keydir = {};

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

function createHintBuffer(file, lineBuffer, key, value) {
  k = new Buffer(key);
  v = new Buffer(value);

  var oldOffset = file.offset;
  file.offset += lineBuffer.length;

  var totalSz = key.length + value.length + sizes.header;

  var hintBufs = new Buffer(sizes.timestamp + sizes.keysize + sizes.offset + sizes.totalsize + key.length)

  //timestamp
  lineBuffer.copy(hintBufs, 0, headerOffsets.timestamp, headerOffsets.timestamp + sizes.timestamp);
  //keysize
  lineBuffer.copy(hintBufs, sizes.timestamp, headerOffsets.keysize, headerOffsets.keysize + sizes.keysize);
  //total size
  hintBufs.writeUInt32BE(totalSz, sizes.timestamp + sizes.keysize);
  //offset
  hintBufs.writeDoubleBE(oldOffset, sizes.timestamp + sizes.keysize + sizes.totalsize);
  //key
  k.copy(hintBufs, sizes.timestamp + sizes.keysize + sizes.totalsize + sizes.offset);

  return hintBufs;
}


function createStringBuffer(k, v) {
  return createBuffer(new Buffer(k), new Buffer(v));
}

describe('HintFileParser', function() {
  before(function (done) {
    require('rimraf')(directory, done);
  });

  describe('.parse', function() {
    before(function(done) {
      var count = 0;
      var max = 10;
      for (var i = 0; i < max; i++) {
        DataFile.create(directory, function(err, file) {
          var val = new Buffer(500);
          val.fill('v');
          var buf = createStringBuffer('hello' + i, val.toString());
          file.write(buf);
          var buf = createStringBuffer('hello1' + i, val.toString());
          file.write(buf);
          var buf = createStringBuffer('hello2' + i, val.toString());
          file.write(buf);
          var oldOffset = file.offset;
          var hintBufs = createHintBuffer(file, buf, 'hello' + i, val.toString());
          file.writeHintFile(hintBufs, function(err) {
            if (err) {
              if (cb) cb(err);
              return;
            }
            file.hintCrc = crc32(hintBufs, file.hintCrc);
            file.hintOffset += hintBufs.length;

            var entry = new KeyDirEntry();
            entry.fileId = file.timestamp;
            entry.valueSize = val.length;
            entry.valuePosition = oldOffset + sizes.header + ('hello' + i).length;
            entry.timestamp = Date.now();

            keydir[k] = entry;

            count++;
            if (count === max) {
              done();
            }
          });
          files.push(file);
          arr.push(file.filename);
        });
      }
    });

    it('parses hint file entries', function(done) {
      HintFileParser.parse(directory, arr, keydir, function(err) {
        assert(!!Object.keys(keydir).length);
        done();
      });
    });

    it('fires the callback even with an empty file array', function(done) {
      HintFileParser.parse(directory, [], keydir, function(err) {
        assert(!err);
        done();
      });
    });

    after(function(done) {
      var active = files.length;
      var callback = function () {
        active--;
        if (active === 0) {
          done();
        }
      }

      files.forEach(function(file) {
        file.closeForWriting(callback);
      });
    });
  });
});
