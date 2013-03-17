var fs = require('fs');

var sizes = {
  timestamp: 8,
  keysize: 2,
  valsize: 4,
  crc: 4,
  header: 4 + 8 + 2 + 4, // crc + timestamp + keysize + valsize
  offset: 16,
  totalsize: 4
};

var stream = fs.createReadStream('medea/2.medea.hint');

var bufs = [];
var len = 0;
stream.on('data', function(chunk) {
  bufs.push(chunk);
  len += chunk.length;
});

stream.on('end', function() {
  var all = new Buffer(len);
  var ofs = 0;
  bufs.forEach(function(buf) {
    buf.copy(all, ofs, 0, buf.length);
    ofs += buf.length;
  });

  read(all, 0);
});

stream.resume();

function read(data, offset) {
  if (offset + sizes.crc === data.length) {
    console.log('hint file crc:', data.slice(offset, offset + sizes.crc).toString('hex'));
    console.log('done!');
    return;
  }
  var header = data.slice(offset, offset + sizes.timestamp + sizes.keysize + sizes.totalsize + sizes.offset);
  var timestamp = header.readDoubleBE(0);
  var keysz = header.readUInt16BE(sizes.timestamp);
  var totalsz = header.readUInt32BE(sizes.timestamp + sizes.keysize);
  var offsetField = header.readDoubleBE(sizes.timestamp + sizes.keysize + sizes.totalsize);

  console.log('timestamp:', timestamp);
  console.log('key size:', keysz);
  console.log('total size:', totalsz);
  console.log('offset:', offsetField);

  var key = data.slice(offset + header.length, offset + header.length + keysz);
  console.log('key:', key.toString());
  console.log('--');

  read(data, offset + header.length + keysz);
};

