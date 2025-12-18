#include "LsmTree.h"
#include "MemTable.h"
#include <filesystem>
#include <format>
#include <optional>
#include <print>
#include <shared_mutex>
namespace lsm_storage_engine {
std::optional<std::string> LsmTree::get(const std::string_view key) {
  // Reads don't block other reads, but block writes.
  std::shared_lock lock(rwlock_);
  std::optional<std::string> val;
  if ((val = mem_table_.get(key))) {
    return *val;
  }
  return std::nullopt;
}
void LsmTree::put(const std::string &key, const std::string &value) {
  std::string msg = std::format("p {} {}\n", key, value);

  // Lock to ensure these two operations are atomic.
  std::unique_lock lock(rwlock_);
  wal_.write(msg);
  mem_table_.put(key, value);
  if (mem_table_.should_flush()) {
    int i{0};
    auto path = std::filesystem::path{"test.sst"};
    while (std::filesystem::exists(path)) {
      path = std::to_string(i) + "test.sst";
      i++;
    }
    auto res = mem_table_.flush_to_disk(path);
    mem_table_.clear();
    wal_.clear();
    // 1. Create SSTable
    // 2. Write contents of memtable to it
    // 3. Clear the WAL
  }
}
} // namespace lsm_storage_engine
