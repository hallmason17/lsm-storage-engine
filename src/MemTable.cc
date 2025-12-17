#include "MemTable.h"
#include <iostream>
namespace lsm_storage_engine {

std::optional<std::string> MemTable::get(const std::string_view key) {
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
  std::cout << size_ << "\n";
  map_.insert_or_assign(std::move(key), std::move(value));
}
void MemTable::restore_from_wal(const std::filesystem::path &wal_path) {}
} // namespace lsm_storage_engine
