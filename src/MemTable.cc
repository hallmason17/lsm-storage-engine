#include "MemTable.h"
#include <shared_mutex>
namespace lsm_storage_engine {

std::optional<std::string> MemTable::get(const std::string_view key) const {
  std::shared_lock lk(rwlock_);
  auto it = map_.find(std::string(key));
  if (it != map_.end()) {
    return it->second;
  }
  return std::nullopt;
}
void MemTable::put(std::string key, std::string value) {
  std::unique_lock lk(rwlock_);
  map_.insert_or_assign(std::move(key), std::move(value));
}
void MemTable::restore_from_wal(const std::filesystem::path &wal_path) {}
} // namespace lsm_storage_engine
