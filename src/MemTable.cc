#include "MemTable.h"
#include "StorageError.h"
#include <cstdint>
#include <fstream>
#include <print>
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
    const std::string_view oldval = map_.find(key)->second;
    size_ -= oldval.size();
    size_ += value.size();
  } else {
    size_ += (key.size() + value.size());
  }
  map_.insert_or_assign(std::move(key), std::move(value));
}

void MemTable::restore_from_wal(const std::filesystem::path &wal_path) {
  std::print("{}", wal_path.c_str());
}

std::expected<void, StorageError>
MemTable::flush_to_disk(const std::filesystem::path &path) {
  std::ofstream of(path, std::ios::binary | std::ios::app);
  if (!of) {
    return std::unexpected(StorageError::file_open(path));
  }
  for (const auto &[key, val] : map_) {
    of << static_cast<uint32_t>(key.size()) << static_cast<uint32_t>(val.size())
       << key.data() << val.data();
  }
  if (!of) {
    return std::unexpected(StorageError::file_write(path));
  }
  return {};
}
} // namespace lsm_storage_engine
