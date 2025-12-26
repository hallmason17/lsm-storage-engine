# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LSM-tree storage engine implementation in C++23. Learning project to understand database internals (RocksDB, LevelDB).

## Build Commands

```bash
# Build (from repo root)
cmake -B build
cmake --build build

# Build with debug mode (enables sanitizers)
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build

# Run tests
./build/test/lsm_test

# Run main executable
./build/lsm
```

## Architecture

**Namespace:** `lsm_storage_engine`

**Write path:** WAL -> MemTable -> SSTable (on flush)
**Read path:** MemTable -> SSTables (newest to oldest)

### Core Components

- **LsmTree** (`src/LsmTree.h`): Main engine coordinating WAL, MemTable, and SSTables. Uses `std::shared_mutex` for thread safety. Restores memtable from WAL on startup.

- **MemTable** (`src/MemTable.h`): In-memory sorted key-value store backed by `std::map`. Tracks size in bytes. Flushes to SSTable when exceeding threshold (4 KB).

- **WAL** (`src/Wal.h`): Write-ahead log using POSIX syscalls (`open`, `write`, `fsync`). Binary format: `[keylen:4][valuelen:4][key][value][xxhash32:4]`. Guarantees durability via `fsync()`.

- **SSTable** (`src/SSTable.h`): Immutable on-disk sorted string table. Uses mmap for reads. Factory methods `create()` and `open()` return `std::expected`. Move-only type with RAII file handle management.
  - **Header:** `[min_key_len:4][min_key][max_key_len:4][max_key]` — enables key range filtering in `get()`
  - **Footer:** `[index_offset:8][index_size:8][num_index_entries:8][magic:8]` — magic number `0xDEADBEEF` for validation
  - **Entries:** `[keylen:4][valuelen:4][key][value][xxhash32:4]`

- **StorageError** (`src/StorageError.h`): Error type for `std::expected<T, StorageError>` returns.

### Key Patterns

- **Error handling:** Use `std::expected<T, StorageError>` instead of exceptions
- **Thread safety:** `std::shared_mutex` for read-write locking
- **Resource management:** RAII for file handles; move-only types where appropriate

## Test Structure

Tests use GoogleTest (v1.14.0, fetched via CMake):
- `test/MemTableTest.cc` - MemTable operations and flush-to-SSTable
- `test/SSTableTest.cc` - SSTable read/write and header/footer validation
- `test/WalTest.cc` - WAL durability
- `test/LsmTreeTest.cc` - Integration tests including compaction
