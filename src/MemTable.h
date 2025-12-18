#pragma once
#include "Constants.h"
#include "StorageError.h"
#include <expected>
#include <filesystem>
#include <map>
#include <optional>
namespace lsm_storage_engine {
class MemTable {
public:
  MemTable()
      : size_(0), flush_threshold_(lsm_constants::kMemTableFlushThreshold) {}
  std::optional<std::string> get(const std::string_view key) const;
  void put(std::string key, std::string value);
  void restore_from_wal(const std::filesystem::path &wal_path);
  const size_t size() const { return size_; }
  bool should_flush() const { return size() > flush_threshold_; }
  void clear() {
    map_.erase(map_.begin(), map_.end());
    size_ = 0;
  }
  std::expected<void, StorageError> flush_to_disk(std::filesystem::path &path);

private:
  std::map<std::string, std::string> map_;
  size_t size_;
  size_t flush_threshold_;
  // TODO: Add a refcount
};
} // namespace lsm_storage_engine
