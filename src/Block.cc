#include "Block.h"
#include "utils/CheckSum.h"
#include <cstdint>
namespace lsm_storage_engine {
size_t Block::append(const std::string_view key, const std::string_view value) {
  std::vector<std::byte> write_buffer;
  auto keylen = static_cast<uint32_t>(key.size());
  auto valuelen = static_cast<uint32_t>(value.size());

  auto append = [&write_buffer](const void *d, size_t len) {
    auto data = reinterpret_cast<const std::byte *>(d);
    write_buffer.insert(write_buffer.end(), data, data + len);
  };

  append(&keylen, sizeof(keylen));
  append(&valuelen, sizeof(valuelen));
  append(key.data(), key.size());
  append(value.data(), value.size());

  auto cs = hash32({reinterpret_cast<const char *>(write_buffer.data()),
                    write_buffer.size()});

  append(&cs, sizeof(cs));

  auto size = write_buffer.size();

  data_.append_range(std::move(write_buffer));

  if (data_.empty()) {
    first_key_ = std::string{key};
  }
  return size;
}

} // namespace lsm_storage_engine
