#include <cstdint>
#include <string_view>
#include <xxhash.h>

inline uint32_t hash32(const std::string_view data) {
  return ::XXH32(data.data(), data.size(), 0);
  ;
}
