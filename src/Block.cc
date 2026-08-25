#include "Block.h"
#include "Constants.h"
#include "utils/CheckSum.h"
#include <cstdint>
#include <cstring>
namespace lsm {

size_t Block::encoded_size(std::string_view key, std::string_view value) {
  return 2 * sizeof(uint32_t) + key.size() + value.size() + sizeof(uint32_t);
}

size_t Block::append(const std::string_view key, const std::string_view value) {
  const auto rec_size = encoded_size(key, value);
  if (!data_.empty() && data_.size() + rec_size > constants::kBlockSize) {
    return 0;
  }

  std::vector<std::byte> write_buffer;
  auto keylen = static_cast<uint32_t>(key.size());
  auto valuelen = static_cast<uint32_t>(value.size());

  auto append_bytes = [&write_buffer](const void *d, size_t len) {
    auto data = reinterpret_cast<const std::byte *>(d);
    write_buffer.insert(write_buffer.end(), data, data + len);
  };

  append_bytes(&keylen, sizeof(keylen));
  append_bytes(&valuelen, sizeof(valuelen));
  append_bytes(key.data(), key.size());
  append_bytes(value.data(), value.size());

  auto cs = hash32({reinterpret_cast<const char *>(write_buffer.data()),
                    write_buffer.size()});

  append_bytes(&cs, sizeof(cs));

  if (!first_key_) {
    first_key_ = std::string{key};
  }
  data_.append_range(write_buffer);
  return rec_size;
}

std::expected<std::optional<std::pair<std::string, std::string>>, StorageError>
Block::decode_entry(std::span<const std::byte> data, size_t &offset) {
  if (offset >= data.size()) {
    return std::nullopt;
  }

  constexpr size_t kLenBytes = 2 * sizeof(uint32_t);
  if (offset + kLenBytes > data.size()) {
    return std::unexpected(StorageError{
        .kind = StorageError::Kind::Corruption,
        .message = "Truncated block entry",
        .path = {},
    });
  }

  uint32_t keylen{0};
  uint32_t valuelen{0};
  ::memcpy(&keylen, data.data() + offset, sizeof(keylen));
  ::memcpy(&valuelen, data.data() + offset + sizeof(uint32_t),
           sizeof(valuelen));

  const size_t entry_size =
      2 * sizeof(uint32_t) + keylen + valuelen + sizeof(uint32_t);
  if (offset + entry_size > data.size()) {
    return std::unexpected(StorageError{
        .kind = StorageError::Kind::Corruption,
        .message = "Block entry extends past block",
        .path = {},
    });
  }

  std::string key(keylen, '\0');
  std::string value(valuelen, '\0');
  uint32_t file_checksum{0};
  ::memcpy(key.data(), data.data() + offset + kLenBytes, keylen);
  ::memcpy(value.data(), data.data() + offset + kLenBytes + keylen, valuelen);
  ::memcpy(&file_checksum,
           data.data() + offset + kLenBytes + keylen + valuelen,
           sizeof(file_checksum));

  auto checksum = hash32({reinterpret_cast<const char *>(data.data() + offset),
                          kLenBytes + keylen + valuelen});
  if (file_checksum != checksum) {
    return std::unexpected(StorageError{
        .kind = StorageError::Kind::Corruption,
        .message = "Checksum mismatch",
        .path = {},
    });
  }

  offset += entry_size;
  return {{{std::move(key), std::move(value)}}};
}

std::expected<std::optional<std::string>, StorageError>
Block::find(std::span<const std::byte> data, std::string_view key) {
  size_t offset = 0;
  while (offset < data.size()) {
    auto entry = decode_entry(data, offset);
    if (!entry) {
      return std::unexpected(entry.error());
    }
    if (!entry->has_value()) {
      break;
    }
    const auto &entry_key = entry->value().first;
    if (entry_key == key) {
      return entry->value().second;
    }
    if (entry_key > key) {
      return std::nullopt;
    }
  }
  return std::nullopt;
}

} // namespace lsm
