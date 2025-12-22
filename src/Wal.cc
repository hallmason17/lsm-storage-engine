#include "Wal.h"
#include "StorageError.h"
#include <cassert>
#include <expected>
#include <fcntl.h>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

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

std::expected<void, StorageError> Wal::write(std::string_view message) {
  const char *data = message.data();
  size_t remaining = message.size();

  assert(fd_ > -1);

  while (remaining > 0) {
    ssize_t written = ::write(fd_, data, remaining);
    if (written == -1) {
      return std::unexpected{StorageError::file_write(path())};
    }
    data += written;
    remaining -= static_cast<size_t>(written);
  }

  return sync();
}

std::expected<void, StorageError> Wal::sync() {
  if (::fsync(fd_) == -1) {
    return std::unexpected{StorageError::file_write(path())};
  }
  return {};
}

std::expected<void, StorageError> Wal::clear() {
  if (::ftruncate(fd_, 0) == -1) {
    return std::unexpected{StorageError::file_write(path())};
  }
  return {};
}

} // namespace lsm_storage_engine
