#include "LsmTree.h"
#include <optional>
namespace lsm_storage_engine {
std::optional<std::string> LsmTree::get(const std::string_view key) {
  return std::nullopt;
}
void LsmTree::put(const std::string &key, const std::string &value) {}
} // namespace lsm_storage_engine
