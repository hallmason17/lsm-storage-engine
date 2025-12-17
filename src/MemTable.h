#pragma once
#include <filesystem>
#include <map>
#include <optional>
#include <shared_mutex>
namespace lsm_storage_engine {
class MemTable {
public:
  std::optional<std::string> get(const std::string_view key) const;
  void put(std::string key, std::string value);
  void restore_from_wal(const std::filesystem::path &wal_path);

private:
  std::map<std::string, std::string> map_;
  std::shared_mutex rwlock_;
};
} // namespace lsm_storage_engine
