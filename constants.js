exports.sizes = {
  timestamp: 8,
  keysize: 2,
  valsize: 4,
  crc: 4,
  header: 4 + 8 + 2 + 4, // crc + timestamp + keysize + valsize
  offset: 16,
  totalsize: 4
};

/**
 * [crc][timestamp][keysz][valuesz]
 */
exports.headerOffsets = {
  crc: 0,
  timestamp: 4,
  keysize: 12,
  valsize: 14
}

exports.writeCheck = {
  fresh: 1,
  wrap: 2,
  ok: 3
};

exports.fsflags = {
  O_RDONLY: 0x0,
  O_CREAT: 0x100,
  O_EXCL: 0x200,
  O_RDWR: 0x02,
  O_SYNC: 0x1000
};

exports.tombstone = new Buffer('medea_tombstone');