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

var stream = fs.createReadStream('medea/1.medea.data');

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
  if (offset + sizes.header > data.length) {
    console.log('done!');
    return;
  }
  var header = data.slice(offset, offset + sizes.header);
  var crc = header.slice(0, sizes.crc);
  var timestamp = header.readDoubleBE(sizes.crc);
  var keysz = header.readUInt16BE(sizes.crc + sizes.timestamp);
  var valuesz = header.readUInt32BE(sizes.crc + sizes.timestamp + sizes.keysize);

  console.log('crc:', crc.toString('hex'));
  console.log('timestamp:', timestamp);
  console.log('key size:', keysz);
  console.log('value size:', valuesz);
  
  var key = data.slice(offset + sizes.header, offset + sizes.header + keysz);
  console.log('key:', key.toString());

  var value = data.slice(offset + sizes.header + keysz, offset + sizes.header + keysz + valuesz);
  console.log('value:', value.toString());
  console.log('value position:', offset + sizes.header + key.length);
  console.log(data.slice(offset + sizes.header + key.length, offset + sizes.header + key.length + value.length).toString());
  console.log('--');

  read(data, offset + sizes.header + keysz + valuesz);
};
