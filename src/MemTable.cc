#include "MemTable.h"
#include "StorageError.h"
#include <cstdint>
#include <expected>
#include <fstream>
#include <print>
#include <sstream>
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

std::expected<void, StorageError>
MemTable::flush_to_disk(const std::filesystem::path &path) {
  std::ofstream of(path, std::ios::binary | std::ios::app);
  if (!of) {
    return std::unexpected(StorageError::file_open(path));
  }
  for (const auto &[key, val] : map_) {
    auto keylen = static_cast<uint32_t>(key.size());
    auto valuelen = static_cast<uint32_t>(val.size());

    of.write(reinterpret_cast<const char *>(&keylen), sizeof(keylen));
    of.write(reinterpret_cast<const char *>(&valuelen), sizeof(valuelen));

    of << key.data() << val.data();
  }
  if (!of) {
    return std::unexpected(StorageError::file_write(path));
  }
  return {};
}
std::expected<void, StorageError>
MemTable::restore_from_wal(const std::filesystem::path &wal_path) {
  std::ifstream wal(wal_path);
  std::string line;
  if (!wal) {
    return std::unexpected(StorageError::file_open(wal_path));
  }
  while (std::getline(wal, line)) {
    std::string cmd, key, value;
    std::istringstream ss(line);
    if (!(ss >> cmd >> key >> value)) {
      break;
    }
    if (cmd == "p") {
      put(key, value);
    } else if (cmd == "d") {
      // TODO: delete
    }
  }
  if (wal.bad()) {
    return std::unexpected(StorageError::file_read(wal_path));
  }
  return {};
}
} // namespace lsm_storage_engine
