#include "SSTable.h"
#include "StorageError.h"
#include "utils/CheckSum.h"
#include <cassert>
#include <cstdint>
#include <cstring>
#include <expected>
#include <fcntl.h>
#include <filesystem>
#include <optional>
#include <print>
#include <span>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
namespace lsm_storage_engine {

SSTable::SSTable(SSTable &&other) noexcept
    : path_{std::move(other.path_)}, fd_{std::exchange(other.fd_, -1)},
      file_pos_{std::exchange(other.file_pos_, 0)},
      mapped_data_{std::exchange(other.mapped_data_, {})},
      file_size_{std::exchange(other.file_size_, 0)} {}

SSTable &SSTable::operator=(SSTable &&other) noexcept {
  if (this != &other) {
    close_file();
    path_ = std::move(other.path_);
    fd_ = std::exchange(other.fd_, -1);
    mapped_data_ = std::exchange(other.mapped_data_, {});
    file_pos_ = std::exchange(other.file_pos_, 0);
    file_size_ = std::exchange(other.file_size_, 0);
  }
  return *this;
}

std::expected<void, StorageError> SSTable::open_file() {
  fd_ = ::open(path_.c_str(), O_CREAT | O_RDWR, 0644);
  if (fd_ == -1) {
    return std::unexpected(StorageError::file_open(path()));
  }
  return {};
}

void SSTable::close_file() {
  if (fd_ != -1) {
    ::close(fd_);
    fd_ = -1;
  }
  if (mapped_data_.data() != nullptr) {
    ::munmap(mapped_data_.data(), file_size_);
  }
}

namespace {
enum class ReadStatus { Ok, Eof };

std::expected<ReadStatus, StorageError>
read_exact(int fd, void *buf, size_t len, const std::filesystem::path &path) {
  assert(fd > -1);
  ssize_t n = ::read(fd, buf, len);
  if (n == 0)
    return ReadStatus::Eof;
  if (n < 0)
    return std::unexpected(StorageError::file_read(path));
  return ReadStatus::Ok;
}
} // namespace

// Maybe the slowest code on earth, lmao.
std::expected<std::optional<std::string>, StorageError>
SSTable::get(std::string_view key) {
  file_pos_ = 0;
  while (true) {
    auto entry = next();
    if (!entry)
      return std::unexpected(entry.error());
    if (!entry->has_value())
      return std::nullopt;

    if (entry->value().first == key) {
      return entry->value().second;
    }
  }
}
std::expected<SSTable, StorageError> SSTable::create() {
  SSTable sst;
  auto now = std::chrono::steady_clock::now().time_since_epoch();
  sst.path_ = std::to_string(now.count()) + ".sst";
  if (!sst.open_file()) {
    return std::unexpected(StorageError::file_open(sst.path_));
  }
  if (!sst.ensure_mapped()) {
    return std::unexpected(StorageError::file_read(sst.path().string()));
  }
  return sst;
}
std::expected<SSTable, StorageError>
SSTable::open(const std::filesystem::path path) {
  SSTable sst{path};
  if (!sst.open_file()) {
    return std::unexpected(StorageError::file_open(sst.path().string()));
  }
  if (!sst.ensure_mapped()) {
    return std::unexpected(StorageError::file_read(sst.path().string()));
  }
  return sst;
}

std::expected<std::optional<std::pair<std::string, std::string>>, StorageError>
SSTable::read_entry_mmap() const {
  if (file_size_ == 0) {
    return std::nullopt;
  }
  if (mapped_data_.data() == nullptr) {
    return std::unexpected(StorageError::file_open(path()));
  }
  if (static_cast<size_t>(file_pos_) >= file_size_) {
    return std::nullopt;
  }
  uint32_t keylen{0};
  uint32_t valuelen{0};
  uint32_t file_checksum{0};
  ::memcpy(&keylen, mapped_data_.data() + file_pos_, sizeof(keylen));
  ::memcpy(&valuelen, mapped_data_.data() + file_pos_ + sizeof(uint32_t),
           sizeof(valuelen));

  size_t entry_size =
      2 * sizeof(uint32_t) + keylen + valuelen + sizeof(uint32_t);
  if (static_cast<size_t>(file_pos_) + entry_size > file_size_) {
    return std::unexpected(StorageError{
        .kind = StorageError::Kind::FileRead,
        .message = "Corrupted SSTable: entry extends past EOF",
        .path = path(),
    });
  }

  std::string k(keylen, '\0');
  std::string val(valuelen, '\0');
  ::memcpy(k.data(), mapped_data_.data() + file_pos_ + 2 * sizeof(uint32_t),
           keylen);
  ::memcpy(val.data(),
           mapped_data_.data() + file_pos_ + 2 * sizeof(uint32_t) + k.size(),
           valuelen);
  ::memcpy(&file_checksum,
           mapped_data_.data() + file_pos_ + 2 * sizeof(uint32_t) + k.size() +
               val.size(),
           sizeof(file_checksum));
  return {{{std::move(k), std::move(val)}}};
}

std::expected<std::optional<std::pair<std::string, std::string>>, StorageError>
SSTable::next() {
  auto entry = ensure_mapped().and_then([&] { return read_entry_mmap(); });

  if (!entry) {
    return std::unexpected(StorageError::file_read(path()));
  }
  if (!entry->has_value())
    return std::nullopt;
  auto &[k, v] = entry->value();

  // [keysize][valuesize][key][val][checksum]
  file_pos_ += sizeof(uint32_t) * 2 + k.size() + v.size() + sizeof(uint32_t);
  return {{{std::move(k), std::move(v)}}};
}
std::expected<std::optional<std::pair<std::string, std::string>>, StorageError>
SSTable::read_entry() const {
  ::lseek(fd_, file_pos_, SEEK_SET);
  uint32_t keylen{0};
  uint32_t valuelen{0};
  uint32_t file_checksum{0};

  auto status = read_exact(fd_, &keylen, sizeof(keylen), path_);
  if (!status)
    return std::unexpected(status.error());
  if (*status == ReadStatus::Eof)
    return {{}};

  auto res = read_exact(fd_, &valuelen, sizeof(valuelen), path_);
  assert(res.has_value());

  std::string k(keylen, '\0');
  std::string val(valuelen, '\0');

  res = read_exact(fd_, k.data(), keylen, path_);
  assert(res.has_value());
  res = read_exact(fd_, val.data(), valuelen, path_);
  assert(res.has_value());
  res = read_exact(fd_, &file_checksum, sizeof(file_checksum), path_);
  assert(res.has_value());

  auto checksum = calc_crc32(std::string_view(
      reinterpret_cast<const char *>(&keylen), sizeof(keylen)));
  checksum =
      calc_crc32(std::string_view(reinterpret_cast<const char *>(&valuelen),
                                  sizeof(valuelen)),
                 checksum);
  checksum = calc_crc32(k, checksum);
  checksum = calc_crc32(val, checksum);
  if (file_checksum != checksum) {
    return std::unexpected(StorageError{
        .kind = StorageError::Kind::FileRead,
        .message = "Checksum mismatch",
        .path = path(),
    });
  }

  return {{{std::move(k), std::move(val)}}};
}
std::expected<void, StorageError>
SSTable::write_entry(const std::string_view key, const std::string_view value) {
  auto keylen = static_cast<uint32_t>(key.size());
  auto valuelen = static_cast<uint32_t>(value.size());

  if (::write(fd_, reinterpret_cast<const char *>(&keylen), sizeof(keylen)) !=
      sizeof(keylen)) {
    return std::unexpected(StorageError::file_write(path()));
  }
  if (::write(fd_, reinterpret_cast<const char *>(&valuelen),
              sizeof(valuelen)) != sizeof(valuelen)) {
    return std::unexpected(StorageError::file_write(path()));
  }
  if (::write(fd_, key.data(), key.size()) !=
      static_cast<ssize_t>(key.size())) {
    return std::unexpected(StorageError::file_write(path()));
  }
  if (::write(fd_, value.data(), value.size()) !=
      static_cast<ssize_t>(value.size())) {
    return std::unexpected(StorageError::file_write(path()));
  }

  auto checksum = calc_crc32(std::string_view(
      reinterpret_cast<const char *>(&keylen), sizeof(keylen)));
  checksum =
      calc_crc32(std::string_view(reinterpret_cast<const char *>(&valuelen),
                                  sizeof(valuelen)),
                 checksum);
  checksum = calc_crc32(key, checksum);
  checksum = calc_crc32(value, checksum);

  if (::write(fd_, reinterpret_cast<const char *>(&checksum),
              sizeof(checksum)) != sizeof(checksum)) {
    return std::unexpected(StorageError::file_write(path()));
  }
  return {};
}
std::expected<void, StorageError> SSTable::ensure_mapped() {
  if (mapped_data_.data() == nullptr) {
    file_size_ = std::filesystem::file_size(path());
    if (file_size_ > 0) {
      void *addr = ::mmap(NULL, file_size_, PROT_READ, MAP_PRIVATE, fd_, 0);
      if (addr == MAP_FAILED) {
        return std::unexpected(StorageError::file_read(path_.string()));
      }
      mapped_data_ =
          std::span<std::byte>{static_cast<std::byte *>(addr), file_size_};
    }
  }
  return {};
}
} // namespace lsm_storage_engine
