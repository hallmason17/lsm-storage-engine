#pragma once
#include "MemTable.h"
#include "SSTable.h"
#include "Wal.h"
#include <optional>
#include <shared_mutex>
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
    mem_table_.restore_from_wal(wal_.path());
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

private:
  MemTable mem_table_;
  Wal wal_;

  /**
   * SSTables ordered newest to oldest.
   * Will have to revisit once I get to compaction.
   */
  std::vector<SSTable> ss_tables_;

  /// Basic RWLock for multithreaded access.
  std::shared_mutex rwlock_;
};
} // namespace lsm_storage_engine
