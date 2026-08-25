#pragma once
#include <cstddef>
namespace lsm {
namespace constants {
// TODO: Add to configuration.
constexpr size_t kMemTableFlushThreshold = 1UZ << 19;
constexpr size_t kMagicNumber = 0xDEADBEEF;
constexpr size_t kIndexSpace = 64;
// 4KB blocks
constexpr size_t kBlockSize = 4096;
// Group-commit WAL: fdatasync after this much unsynced data.
constexpr size_t kWalSyncThreshold = 1UZ << 16;
}  // namespace constants
};  // namespace lsm
