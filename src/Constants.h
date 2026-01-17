#pragma once
#include <cstddef>
namespace lsm_storage_engine {
namespace lsm_constants {
// TODO: Add to configuration.
constexpr size_t kMemTableFlushThreshold = 1UZ << 19;
constexpr size_t kMagicNumber = 0xDEADBEEF;
// 4KB blocks
constexpr size_t kBlockSize = 4096;
} // namespace lsm_constants
}; // namespace lsm_storage_engine
