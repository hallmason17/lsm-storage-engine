#include <cstdint>
#include <string_view>
#include <zlib.h>

inline uint32_t calc_crc32(const std::string_view data, uint32_t prev = 0L) {
  return static_cast<uint32_t>(
      ::crc32(prev, reinterpret_cast<const Bytef *>(data.data()),
              static_cast<uint32_t>(data.size())));
}
