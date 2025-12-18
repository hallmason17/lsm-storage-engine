#include "LsmTree.h"
int main() {
  using namespace lsm_storage_engine;
  LsmTree lsm;
  lsm.put("foo", "bar");
  auto bar = lsm.get("foo");
  lsm.put("foo", "bar11");
  for (int i = 0; i < 100; i++) {
    auto key = "key" + std::to_string(i);
    auto value = "value" + std::to_string(i);
    lsm.put(key, value);
  }
  return 0;
}
