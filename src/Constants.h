#pragma once
#include <cstddef>
namespace lsm_storage_engine {
namespace lsm_constants {
constexpr unsigned long kMemTableFlushThreshold = 1UZ << 8;
}
}; // namespace lsm_storage_engine
