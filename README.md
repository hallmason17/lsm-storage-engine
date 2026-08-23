# LSM-Tree Storage Engine

C++23 implementation of a Log-Structured Merge-tree.

![LSM-Tree Architecture](./docs/architecture.png)

## Build

```bash
cmake -B build && cmake --build build
./build/test/lsm_test    # tests
./build/lsm              # 10K puts/gets benchmark
```

Debug build enables ASan + UBSan:
```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug
```

## Performance notes

- mmap for SSTable reads eliminates syscall overhead (read() was initially 99% of time in profiles)
- Switched from zlib CRC32 to xxHash (CRC32 was 70% of CPU time in profiles)
- Buffered writes batch data before hitting disk

## References

- CMU 15-445
- Designing Data-Intensive Applications (Kleppmann)
- LevelDB source
