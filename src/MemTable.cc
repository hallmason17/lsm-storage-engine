#include "MemTable.h"
#include "Constants.h"
#include "StorageError.h"
#include "utils/CheckSum.h"
#include <cassert>
#include <expected>
#include <filesystem>
#include <fstream>
#include <ios>
#include <sstream>
#include <sys/fcntl.h>
#include <unistd.h>
namespace lsm_storage_engine {

std::optional<std::string> MemTable::get(const std::string_view key) const {
  auto it = map_.find(std::string(key));
  if (it != map_.end()) {
    return it->second;
  }
  return std::nullopt;
}
void MemTable::put(std::string key, std::string value) {
  if (map_.contains(key)) {
    const std::string_view oldval = map_.find(key)->second;

    assert(size_ >= oldval.size());

    size_ -= oldval.size();
    size_ += value.size();
  } else {
    size_ += (key.size() + value.size());
  }
  map_.insert_or_assign(std::move(key), std::move(value));
}

std::expected<void, StorageError> MemTable::flush_to_sst(SSTable &sst) {
  // Handle empty table - write valid SSTable with empty key range
  std::string min_key;
  std::string max_key;
  size_t bytes_written{0};

  if (!map_.empty()) {
    min_key = map_.begin()->first;
    max_key = map_.rbegin()->first;
  }

  SSTable::Header header{min_key, max_key};
  if (auto res = sst.write_header(std::move(header)); !res) {
    return std::unexpected{res.error()};
  }
  bytes_written += sst.header().size;

  BloomFilter bloom_filter{map_.size()};
  for (const auto &[key, val] : map_) {
    bloom_filter.add(std::string_view{key});
  }

  auto bf_res = sst.write_bloom_filter(std::move(bloom_filter));
  if (!bf_res) {
    return std::unexpected{bf_res.error()};
  }
  bytes_written += bf_res.value();

  size_t i = 0;

  for (const auto &[key, val] : map_) {
    auto result = sst.write_entry(key, val);
    if (!result) {
      return std::unexpected(result.error());
    }

    if (i % lsm_constants::kIndexSpace == 0) {
      sst.index().emplace_back(std::string{key}, bytes_written);
    }
    bytes_written += result.value();
    i++;
  }

  SSTable::Footer footer;
  footer.index_offset = bytes_written;

  auto result = sst.write_index();
  if (!result) {
    return std::unexpected(result.error());
  }
  bytes_written += result.value();
  footer.index_size = result.value();
  footer.num_index_entries = sst.index().size();

  if (auto res = sst.write_footer(footer); !res) {
    return std::unexpected{res.error()};
  }
  return {};
}
std::expected<void, StorageError>
MemTable::restore_from_wal(const std::filesystem::path &wal_path) {
  if (!std::filesystem::exists(wal_path)) {
    return {};
  }

  int fd = ::open(wal_path.c_str(), O_RDONLY, 0644);
  if (fd == -1) {
    return std::unexpected(StorageError::file_open(wal_path));
  }

  while (true) {
    uint32_t keylen{0};
    uint32_t valuelen{0};
    uint32_t checksum{0};
    auto bytes_read = ::read(fd, &keylen, sizeof(keylen));
    if (bytes_read == 0) {
      break;
    }
    if (bytes_read != sizeof(keylen)) {
      ::close(fd);
      return std::unexpected(StorageError::file_read(wal_path));
    }
    if (::read(fd, &valuelen, sizeof(valuelen)) != sizeof(valuelen)) {
      ::close(fd);
      return std::unexpected(StorageError::file_read(wal_path));
    }
    std::string key(keylen, '\0');
    std::string value(valuelen, '\0');
    if (::read(fd, key.data(), keylen) != static_cast<ssize_t>(keylen)) {
      ::close(fd);
      return std::unexpected(StorageError::file_read(wal_path));
    }
    if (::read(fd, value.data(), valuelen) != static_cast<ssize_t>(valuelen)) {
      ::close(fd);
      return std::unexpected(StorageError::file_read(wal_path));
    }
    if (::read(fd, &checksum, sizeof(checksum)) != sizeof(checksum)) {
      ::close(fd);
      return std::unexpected(StorageError::file_read(wal_path));
    }
    std::vector<std::byte> buf;
    auto append = [&buf](const void *d, size_t len) {
      auto data = reinterpret_cast<const std::byte *>(d);
      buf.insert(buf.end(), data, data + len);
    };

    append(&keylen, sizeof(keylen));
    append(&valuelen, sizeof(valuelen));
    append(key.data(), key.size());
    append(value.data(), value.size());

    auto cs = hash32({reinterpret_cast<const char *>(buf.data()), buf.size()});
    if (checksum != cs) {
      ::close(fd);
      return std::unexpected(
          StorageError{.kind = StorageError::Kind::Corruption,
                       .message = "Corrupted WAL entry, checksum mismatch",
                       .path = wal_path});
    }

    put(std::move(key), std::move(value));
  }
  ::close(fd);
  return {};
}
} // namespace lsm_storage_engine
