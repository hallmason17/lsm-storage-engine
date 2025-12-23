#pragma once
#include "MemTable.h"
#include "SSTable.h"
#include "Wal.h"
#include <atomic>
#include <filesystem>
#include <optional>
#include <print>
#include <shared_mutex>
#include <stdexcept>
#include <string_view>
#include <vector>
namespace lsm_storage_engine {

/**
 * @brief Core LSM-tree storage engine.
 *
 * Design:
 *  - Write to WAL first so writes are durable
 *  - One memtable (could add another to double-buffer later)
 *  - Vector of SSTables (no compaction algorithm yet)
 *
 * Write path: WAL -> MemTable -> SSTable (when flushed)
 * Read path: MemTable -> SSTables (newest to oldest)
 */
class LsmTree {
public:
  // What do I even name a WAL?
  LsmTree() : wal_(std::filesystem::path("lsm.wal")) {
    // Restore the memtable from WAL on startup.
    auto result = mem_table_.restore_from_wal(wal_.path());
    if (!result) {
      std::println("{}", result.error().message);
      throw std::runtime_error("Could not restore state from WAL!");
    }
    if (!load_ssts()) {
      throw std::runtime_error("Could not load SSTables!");
    }
    //    std::println("LSM constructed! Memtable size: {}B, Num SSTs: {}",
    //                mem_table_.size(), ss_tables_.size());
  }

  /// Prevent the object from being copied
  LsmTree(const LsmTree &) = delete;
  LsmTree &operator=(const LsmTree &) = delete;

  /// Delete move constructors for shared_mutex
  LsmTree(LsmTree &&) noexcept = delete;
  LsmTree &operator=(LsmTree &&) noexcept = delete;

  /**
   * @brief Retrieve the value of a key
   * @param key The key to look up
   * @return The value if found, std::nullopt otherwise
   */
  std::optional<std::string> get(const std::string_view key);

  /**
   * @brief Insert or update a key-value pair
   * @param key Key to insert/update
   * @param value Value to store
   */
  void put(const std::string &key, const std::string &value);

  /**
   * @brief Remove a key-value pair
   * @param key Key to remove
   */
  void rm(const std::string &key);

  struct Stats {
    unsigned long get_count;
    unsigned long put_count;
    double avg_get_time_us;
    double avg_put_time_us;
    long long max_put_time_us_;
    long long max_get_time_us_;
  };

  /**
   * @brief Get operation timing statistics
   * @return Stats containing counts and average times in microseconds
   */
  Stats stats() const;

private:
  MemTable mem_table_;
  Wal wal_;

  /**
   * SSTables ordered oldest to newest (new SSTables are pushed to back).
   * Will have to revisit once I get to compaction.
   */
  std::vector<SSTable> ss_tables_;

  /// Basic RWLock for multithreaded access.
  std::shared_mutex rwlock_;

  /**
   * @brief Load SSTables associated with this LSM-tree into the ss_tables_
   * vector.
   * @return void on success, StorageError on failure.
   */
  std::expected<void, StorageError> load_ssts();

  /**
   * @brief Update the LSM-tree meta file with SSTable to add it to the
   * database.
   * @return void on success, StorageError on failure.
   */
  std::expected<void, StorageError> update_meta(SSTable &sstable);

  std::expected<void, StorageError> maybe_compact();

  std::expected<void, StorageError> flush_memtable();

  // Timing stats - using atomics for thread-safe updates without holding the
  // main lock
  std::atomic<unsigned long> get_count_{0};
  std::atomic<unsigned long> put_count_{0};
  std::atomic<long long> total_get_time_us_{0};
  std::atomic<long long> total_put_time_us_{0};
  std::atomic<long long> max_put_time_us_{0};
  std::atomic<long long> max_get_time_us_{0};
};
} // namespace lsm_storage_engine
