var fs = require('fs');

var sizes = {
  timestamp: 32,
  keysize: 16,
  valsize: 32,
  crc: 32,
  header: 32 + 32 + 16 + 32, // crc + timestamp + keysize + valsize
  offset: 64,
  totalSize: 32
};

var stream = fs.createReadStream('medea/1.medea.hint');

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
  var header = data.slice(offset, offset + sizes.timestamp + sizes.keysize + sizes.totalsize + sizes.offset);
  console.log(header.length)
  var timestamp = header.readDoubleBE(0);
  console.log(header.length)
  var keysz = header.readDoubleBE(sizes.timestamp);
  var totalsz = header.readDoubleBE(sizes.timestamp + sizes.keysize);

  console.log('timestamp:', timestamp);
  console.log('key size:', keysz);
  console.log('total size:', totalsz);
  console.log('offset', offset);
  console.log(data.length - offset);

  var key = data.slice(offset + header.length, offset + header.length + keysz);
  console.log(key.toString());
  console.log('--');

  read(data, offset + header.length + keysz);
  
  /*
  var key = data.slice(offset + sizes.header, offset + sizes.header + keysz);
  console.log('key:', key.toString());

  var value = data.slice(offset + sizes.header + keysz, offset + sizes.header + keysz + valuesz);
  console.log('value', value.toString());
  console.log('--');

  read(data, offset + sizes.header + keysz + valuesz);
  */
};

