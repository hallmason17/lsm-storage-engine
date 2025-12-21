# LSM Storage Engine

Building an LSM-tree storage engine in C++ from scratch for learning.

## Goal
Understand how databases like RocksDB work by implementing the core concepts myself.

## Status
Implementing everything naively first and optimizing later.

### Completed
- [x] Basic MemTable (get/put, size tracking)
- [x] Write-ahead log (write, clear, fsync durability)
- [x] MemTable flush creates proper SSTables
- [x] SSTable read path (scan SSTables when key not in memtable)
- [x] WAL recovery (rebuild memtable on startup)
- [x] SSTable tracking (which SSTables are part of the system?)
- [x] SSTable compaction/garbage collection

### Remaining for naive v1
- [ ] Delete operation (tombstone markers)

## Building
```bash
mkdir -p build && cd build
cmake ..
make
```

## Learning Resources
- CMU 15-445 Database Systems
- Designing Data-Intensive Applications (Kleppmann)
- LevelDB source code

## Progress
Following along on Twitter: @masonhalla
