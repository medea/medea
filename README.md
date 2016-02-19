# medea

A persistent key-value storage library.

* Can be embedded in Node.js applications.
* Support for `get`, `put`, and `remove` operations.
* Influenced by Basho's [Bitcask](https://github.com/basho/bitcask) key-value store.
* Values can be any string or `Buffer`.
* Supports atomic batch writes.
* Allows snapshots for consistent views of data.

[![Build Status](https://api.travis-ci.org/medea/medea.svg?branch=master)](https://travis-ci.org/medea/medea)  [![Build status](https://ci.appveyor.com/api/projects/status/cim1lbhjdxn33mfh/branch/master?svg=true)](https://ci.appveyor.com/project/kevinswiber/medea/branch/master)


## Contents

- [Example](#example)
- [Install](#install)
- [Usage](#usage)
  - [db.open([directory], callback)](#dbopendirectory-callback)
  - [db.get(key, [snapshot], callback)](#dbgetkey-snapshot-callback)
  - [db.put(key, value, callback)](#dbputkey-value-callback)
  - [db.remove(key, callback)](#dbremovekey-callback)
  - [db.createSnapshot()](#dbcreatesnapshot)
  - [db.createBatch()](#dbcreatebatch)
  - [db.write(batch, callback)](#dbwritebatch-callback)
  - [db.sync(callback)](#dbsynccallback)
  - [db.close(callback)](#dbclosecallback)
  - [db.listKeys(callback)](#dblistkeyscallback)
  - [db.mapReduce(options, callback)](#dbmapreduceoptions-callback)
  - [db.compact(callback)](#dbcompactcallback)
  - [db.destroy(callback) / medea.destroy(director, callback)](#dbdestroycallback--medeadestroydirector-callback)
- [How It Works](#how-it-works)
  - [Keydir](#keydir)
  - [Data Files](#data-files)
  - [Hint Files](#hint-files)
  - [Lock Files](#lock-files)
- [Limitations](#limitations)
- [License](#license)

## Example

```javascript
var medea = require('medea');

var db = medea();

db.open(function(err) {
  db.put('hello', 'world!', function(err) {
    db.get('hello', function(err, val) {
      console.log(val.toString());
      db.close();
    });
  });
});
```

## Install

```bash
$ npm install medea
```

## Usage

### db.open([directory], callback)

Opens a Medea key-value store.

`directory`: Optional. Defaults to `medea` in the current directory.

`callback`: Takes one error parameter.

### db.get(key, [snapshot], callback)

Returns the value associated with the given key.

`key`: identifier to retrieve the value

`snapshot`: a snapshot on which to query.  optional.

`callback`: has the signature `function (err, value)` where `value` is the returned value.

### db.put(key, value, callback) 

Stores a value in Medea.

`key`: identifier

`value`: value associated with the key

`callback`: function that takes an error parameter

### db.remove(key, callback)

Removes an entry from Medea.

`key`: identifier for the item to remove

`callback`: Takes one error parameter.

### db.createSnapshot()

Creates a transient snapshot of the data.  Works with [db.get(key, [snapshot], callback)](#dbgetkey-snapshot-callback).

Returns a Snapshot object.

Example:

```js
db.put('hello', 'world', function(err) {
  var snapshot = db.createSnapshot();
  db.put('hello', 'void', function(err) {
    db.get('hello', snapshot, function(err, val) {
      assert.equal(val.toString, 'world');
    });
  });
});
```

### db.createBatch()

Creates a batch object for atomic writes.  Works with [db.write(batch, callback)](#dbwritebatch-callback).

Returns a WriteBatch object.

### db.write(batch, callback)

Commits `put` and `remove` operations atomically.

`batch`: a WriteBatch object created with [db.createBatch()](#dbcreatebatch).

`callback`: Takes one error parameter.

Example:

```js
var batch = db.createBatch();
batch.put('key1', 'value1');
batch.remove('key2');

db.write(batch, function(err) {
  console.log('batch written atomically');
});
```

### db.sync(callback)

Performs an fsync operation on the active data and hint files.

### db.close(callback)

Closes Medea.

### db.listKeys(callback)

Returns an array of keys from the key-value store.

`callback`: A function that takes two parameters: an error and the array of keys.

### db.mapReduce(options, callback)

Experimental.

Ad-hoc map-reduce queries over the key-value pairs.  The query results are not indexed.  A more robust map-reduce implementation will be provided in the near future.

See `examples/map_reduce.js` for an example. <!--_-->

### db.compact(callback)

Runs a compaction process on the database files.  Reduces size of data on disk.  Deletes old log entries.

`callback`: A function that takes an error parameter.

### db.destroy(callback) / medea.destroy(director, callback)

Destroys the database. If the folder only includes medea-files, the folder is removed also otherwise only the medea-files are removed and the folder is kept.

## How it Works

All Medea key-value stores reside on the file system in a directory.  The directory is made up of data files, hint files, and lock files.  All keys are held in an in-memory keydir for fast lookup.  More explanation is below.

### Keydir

The keydir resides in memory.  It contains a key, an associated data file ID, a value size, and a file offset.  When a `get` is requested, a lookup happens in the keydir.  The keydir entry allows one disk seek for each `get` operation.  When a `put` or `remove` occurs, the value is updated in the keydir after the update occurs on-disk.  The key set must fit in memory.

### Data Files

Format: `{seq}.medea.data` where `{seq}` is an incrementing number.

A new data file is created each time a Medea store is opened or after an active data file reaches a configured size limit.  Data files are opened in an append-only mode and act as a log.  For every `put` and `remove` a new entry is inserted into the currently active data file.  Each entry contains a timestamp, the key size, the value size, the key, and the value itself.  In addition, a CRC32 checksum is calculated, which can be used to test the integrity of the entry.

When existing keys are updated with new values, new entries are appended to the end of the data file.  Note: When values are removed, the entry to the data file looks just like a `put`, but with a special tombstone value.

### Hint Files

Format: `{seq}.medea.hint` where `{seq}` is an incrementing number.

Each data file has an associated hint file.  Entries in a hint file contain a timestamp, a key size, a value size, an offset, and the key itself.  The offset points to the latest value entry for a particular key.  Hint files are used to build a new keydir when a Medea store is opened. This is much quicker than reading through the data files.

### Lock files

Format: `medea.write.lock`

Contains a process ID and the active file path.

## Limitations

Currently, multiple processes cannot access the same data directory.  Run one process per directory.  A workaround has been developed for servers using Node's `cluster` module.  Check out [medea-clusterify](https://github.com/medea/medea-clusterify) to see how it works!

Repeated use leads to fragmentation and empty files. The compaction process needs to be run for cleanup.  See: [db.compact(callback)](#dbcompactcallback).

Medea's design places all keys in memory.  Therefore, the entire key set must fit in memory.  All values are stored on disk.

## License

MIT

Copyright 2013 Apigee Corporation and Contributors
