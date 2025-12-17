#pragma once
#include <filesystem>
#include <map>
#include <optional>
namespace lsm_storage_engine {
class MemTable {
public:
  MemTable() : size_(0) {}
  std::optional<std::string> get(const std::string_view key);
  void put(std::string key, std::string value);
  void restore_from_wal(const std::filesystem::path &wal_path);
  const size_t size() const { return size_; }

private:
  std::map<std::string, std::string> map_;
  size_t size_;
};
} // namespace lsm_storage_engine
