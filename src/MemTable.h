#pragma once
#include "Constants.h"
#include "StorageError.h"
#include <expected>
#include <filesystem>
#include <map>
#include <optional>
namespace lsm_storage_engine {

/**
 * @brief In-memory sorted key-value store for the LSM-tree.
 *
 * MemTable serves as the first destination for all writes in the LSM-tree.
 * Keys are stored in sorted order using std::map, enabling efficient range
 * scans and ordered iteration. When the table exceeds the flush threshold,
 * it should be persisted to disk as an SSTable.
 *
 * Not thread-safe.
 *
 * TODO: switch to Skiplist
 */
class MemTable {
public:
  MemTable()
      : size_(0), flush_threshold_(lsm_constants::kMemTableFlushThreshold) {}

  /**
   * @brief Retrieves the value associated with the given key.
   * @param key The key to look up.
   * @return The value if found, std::nullopt otherwise.
   */
  std::optional<std::string> get(const std::string_view key) const;

  /**
   * @brief Inserts or updates a key-value pair.
   * @param key The key to insert or update.
   * @param value The value to associate with the key.
   */
  void put(std::string key, std::string value);

  /**
   * @brief Restores the MemTable state by replaying a write-ahead log.
   * @param wal_path Path to the WAL file to replay.
   * @return void on success, StorageError on failure.
   */
  std::expected<void, StorageError>
  restore_from_wal(const std::filesystem::path &wal_path);

  /**
   * @brief Returns the approximate size of the MemTable in bytes.
   */
  size_t size() const { return size_; }

  /**
   * @brief Checks whether the MemTable should be flushed to disk.
   * @return true if size exceeds the flush threshold.
   */
  bool should_flush() const { return size() > flush_threshold_; }

  /**
   * @brief Removes all entries and resets size to zero.
   */
  void clear() {
    map_.erase(map_.begin(), map_.end());
    size_ = 0;
  }

  /**
   * @brief Persists the MemTable contents to disk as an SSTable.
   * @param path Destination file path for the SSTable.
   * @return void on success, StorageError on failure.
   */
  std::expected<void, StorageError>
  flush_to_disk(const std::filesystem::path &path);

private:
  std::map<std::string, std::string> map_;
  size_t size_;
  size_t flush_threshold_;
};
} // namespace lsm_storage_engine
