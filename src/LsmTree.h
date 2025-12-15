#include "MemTable.h"
#include "SSTable.h"
#include "Wal.h"
#include <optional>
#include <string_view>
#include <vector>
namespace lsm_storage_engine {
class LsmTree {
public:
  LsmTree() {}

  // Prevent the object from being copied
  LsmTree(const LsmTree &) = delete;
  LsmTree &operator=(const LsmTree &) = delete;

  std::optional<std::string> get(const std::string_view key);
  void put(const std::string key, const std::string value);

private:
  MemTable mem_table_;
  Wal wal_;
  std::vector<SSTable> ss_tables_;
};
} // namespace lsm_storage_engine
