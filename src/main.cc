#include "LsmTree.h"
#include <thread>
int main() {
  using namespace lsm_storage_engine;
  /*
{
  LsmTree lsm;
  lsm.put("foo", "bar");
  auto bar = lsm.get("foo");
  lsm.put("foo", "bar11");
  for (int i = 0; i < 10000; i++) {
    auto key = "key" + std::to_string(i);
    auto value = "value" + std::to_string(i);
    lsm.put(key, value);
  }
  auto s = lsm.stats();
  std::println("Put: {} ops, avg {:.0f}us, max {}us", s.put_count,
               s.avg_put_time_us, s.max_put_time_us_);
}
  */
  LsmTree lsm;
  std::vector<std::thread> threads;
  for (int i = 0; i < 2; i++) {
    threads.emplace_back([&lsm] {
      for (int i = 0; i < 10000; i++) {
        auto key = "key" + std::to_string(i);
        auto val = lsm.get(key);
        if (val) {
          // std::println("{}", *val);
        }
      }
    });
  }
  for (auto &t : threads) {
    t.join();
  }
  auto s = lsm.stats();
  std::println("Get: {} ops, avg {:.0f}us, max {}us", s.get_count,
               s.avg_get_time_us, s.max_get_time_us_);
  return 0;
}
