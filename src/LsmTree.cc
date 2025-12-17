#include "LsmTree.h"
#include <format>
#include <optional>
namespace lsm_storage_engine {
std::optional<std::string> LsmTree::get(const std::string_view key) {
  std::optional<std::string> val;
  if ((val = mem_table_.get(key))) {
    return *val;
  }
  return std::nullopt;
}
void LsmTree::put(const std::string &key, const std::string &value) {
  std::string msg = std::format("p {} {}\n", key, value);
  wal_.write(msg);
  mem_table_.put(key, value);
}
} // namespace lsm_storage_engine
