#include "MemTable.h"
#include "StorageError.h"
#include "utils/CheckSum.h"
#include <cassert>
#include <expected>
#include <fstream>
#include <ios>
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

    assert(size_ >= oldval.size());

    size_ -= oldval.size();
    size_ += value.size();
  } else {
    size_ += (key.size() + value.size());
  }
  map_.insert_or_assign(std::move(key), std::move(value));
}

std::expected<void, StorageError> MemTable::flush_to_sst(SSTable &sst) {
  // Handle empty table - write valid SSTable with empty key range
  std::string min_key;
  std::string max_key;
  if (!map_.empty()) {
    min_key = map_.begin()->first;
    max_key = map_.rbegin()->first;
  }

  SSTable::Header header{min_key, max_key};
  if (auto res = sst.write_header(std::move(header)); !res) {
    return std::unexpected{res.error()};
  }
  for (const auto &[key, val] : map_) {
    auto result = sst.write_entry(key, val);
    if (!result) {
      return std::unexpected(result.error());
    }
  }
  SSTable::Footer footer;
  if (auto res = sst.write_footer(std::move(footer)); !res) {
    return std::unexpected{res.error()};
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
