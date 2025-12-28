#include "Wal.h"
#include "StorageError.h"
#include "utils/CheckSum.h"
#include <cassert>
#include <expected>
#include <fcntl.h>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace lsm_storage_engine {

Wal::Wal(std::filesystem::path filename) : path_{std::move(filename)} {
  if (!open_file()) {
    throw std::runtime_error("Unable to open WAL");
  }
}

Wal::~Wal() { close_file(); }

Wal::Wal(Wal &&other) noexcept
    : path_{std::move(other.path_)}, fd_{std::exchange(other.fd_, -1)} {}

Wal &Wal::operator=(Wal &&other) noexcept {
  if (this != &other) {
    close_file();
    path_ = std::move(other.path_);
    fd_ = std::exchange(other.fd_, -1);
  }
  return *this;
}

std::expected<void, StorageError> Wal::open_file() {
  fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd_ == -1) {
    return std::unexpected{StorageError::file_open(path())};
  }
  return {};
}

void Wal::close_file() {
  if (fd_ != -1) {
    ::close(fd_);
    fd_ = -1;
  }
}

std::expected<void, StorageError> Wal::write(std::string_view key,
                                             std::string_view value) const {
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

  if (::write(fd_, write_buffer.data(), write_buffer.size()) !=
      static_cast<ssize_t>(write_buffer.size())) {
    return std::unexpected(StorageError::file_write(path()));
  }
  return {};
}

std::expected<void, StorageError> Wal::sync() const {
  if (::fsync(fd_) == -1) {
    return std::unexpected{StorageError::file_write(path())};
  }
  return {};
}

std::expected<void, StorageError> Wal::clear() const {
  if (::ftruncate(fd_, 0) == -1) {
    return std::unexpected{StorageError::file_write(path())};
  }
  return {};
}

} // namespace lsm_storage_engine
