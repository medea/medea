var bufferEqual = require('buffer-equal');

var constants = require('./constants');

exports.isTombstone = function (buffer) {
  return bufferEqual(buffer, constants.tombstone);
}
