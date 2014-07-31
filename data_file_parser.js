var fs = require('fs');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var constants = require('./constants');
var DataEntry = require('./data_entry');
var headerOffsets = constants.headerOffsets;
var sizes = constants.sizes;

var DataFileParser = module.exports = function(file) {
  this.file = file;
};
util.inherits(DataFileParser, EventEmitter);

DataFileParser.prototype.parse = function() {
  var self = this;
  var stream = fs.createReadStream(this.file.filename);

  stream.on('error', function(err) {
    self.emit('error', err);
  });

  stream.on('end', function() {
    self.emit('end');
  });

  var waiting = new Buffer(0);
  var curlen = 0;
  var lastHeaderBuf;
  var lastKVLen = -1;
  var state = 'findingHeader';

  stream.on('data', function(chunk) {
    curlen = chunk.length;

    if (waiting.length) {
      /*if (!chunk) {
        chunk = new Buffer(0);
      }*/
      chunk = Buffer.concat([waiting, chunk]);
      curlen = chunk.length;
      waiting = new Buffer(0);
    }

    while (curlen) {
      if (state === 'entryFound') {
        state = 'findingHeader';
      }

      if (curlen < sizes.header && state === 'findingHeader') {
        waiting = chunk;
        chunk = new Buffer(0);
        curlen = 0;
        return;
      }

      if (state === 'headerFound' && lastKVLen > -1 && curlen < lastKVLen) {
        waiting = chunk;
        chunk = new Buffer(0);
        curlen = 0;
        return;
      }

      if (curlen >= sizes.header && state === 'findingHeader') {
        var headerBuf = chunk.slice(0, sizes.header);

        var keylen = headerBuf.readUInt16BE(headerOffsets.keysize);
        var vallen = headerBuf.readUInt32BE(headerOffsets.valsize);
        lastKVLen = keylen + vallen;
        lastHeaderBuf = headerBuf;

        chunk = chunk.slice(headerBuf.length);
        waiting = new Buffer(0);
        curlen = chunk.length;

        state = 'headerFound';
      } else if (curlen >= lastKVLen && state === 'headerFound') {
        var kvBuf = chunk.slice(0, lastKVLen);

        var bufs = Buffer.concat([lastHeaderBuf, kvBuf]);

        var entry = DataEntry.fromBuffer(bufs);
        self.emit('entry', entry);

        chunk = chunk.slice(lastKVLen);
        curlen = chunk.length;
        lastKVLen = -1;
        lastHeaderBuf = null;

        state = 'entryFound';
      } else {
        if (state === 'entryFound') {
          state = 'findingHeader';
        } else {
          waiting = chunk;
          chunk = new Buffer(0);
          curlen = 0;
        }
      }
    }

  });
};
