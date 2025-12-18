#include "MemTable.h"
#include "StorageError.h"
#include <cstdint>
#include <fstream>
#include <iostream>
namespace lsm_storage_engine {

std::optional<std::string> MemTable::get(const std::string_view key) const {
  auto it = map_.find(std::string(key));
  if (it != map_.end()) {
    return it->second;
  }
  return std::nullopt;
}
void MemTable::put(std::string key, std::string value) {
  if (map_.contains(key)) {
    std::string val = map_.find(key)->second;
    int diff = value.size() - val.size();
    size_ += diff;
  } else {
    size_ += (key.size() + value.size());
  }
  std::cout << size_ << "/" << flush_threshold_ << "B\n";
  map_.insert_or_assign(std::move(key), std::move(value));
}

void MemTable::restore_from_wal(const std::filesystem::path &wal_path) {}

std::expected<void, StorageError>
MemTable::flush_to_disk(const std::filesystem::path &path) {
  std::ofstream of(path, std::ios::binary | std::ios::app);
  if (!of) {
    return std::unexpected(StorageError::file_open(path));
  }
  for (auto it = map_.begin(); it != map_.end(); ++it) {
    auto keylen = static_cast<uint32_t>(it->first.size());
    auto valuelen = static_cast<uint32_t>(it->second.size());
    of << keylen << valuelen << it->first.data() << it->second.data();
  }
  if (!of) {
    return std::unexpected(StorageError::file_write(path));
  }
  return {};
}
} // namespace lsm_storage_engine
