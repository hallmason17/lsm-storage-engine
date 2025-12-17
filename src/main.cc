#include "LsmTree.h"
int main() {
  using namespace lsm_storage_engine;
  LsmTree lsm;
  lsm.put("foo", "bar");
  auto bar = lsm.get("foo");
  lsm.put("foo", "bar11");
  return 0;
}
