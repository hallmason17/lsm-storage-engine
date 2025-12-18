#pragma once
#include <cstddef>
namespace lsm_storage_engine {
namespace lsm_constants {
constexpr size_t kMemTableFlushThreshold = 1UZ << 8;
}
}; // namespace lsm_storage_engine
