var bufferEqual = require('buffer-equal');

var constants = require('./constants');

exports.isTombstone = function (buffer) {
  return bufferEqual(buffer, constants.tombstone);
}

exports.dumpMapKeys = function (map) {
  var out = new Array(map.size);
  var i = 0;
  map.forEach(function (value, key) {
    out[i++] = key;
  });
  return out;
}
