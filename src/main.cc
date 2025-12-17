#include "LsmTree.h"
#include <iostream>
int main() {
  using namespace lsm_storage_engine;
  LsmTree lsm;
  lsm.put("foo", "bar");
  auto bar = lsm.get("foo");
  std::cout << *bar;
  return 0;
}
