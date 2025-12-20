#pragma once
namespace lsm_storage_engine {
namespace lsm_constants {
// TODO: Add to configuration.
constexpr unsigned long kMemTableFlushThreshold = 1UZ << 16;
} // namespace lsm_constants
}; // namespace lsm_storage_engine
