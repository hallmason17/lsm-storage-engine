#include "Wal.h"
#include <fcntl.h>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

namespace lsm_storage_engine {

Wal::Wal(std::filesystem::path filename) : path_{std::move(filename)} {
  open_file();
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

void Wal::open_file() {
  // O_WRONLY: write only
  // O_CREAT: create if doesn't exist
  // O_APPEND: all writes go to end of file
  fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd_ == -1) {
    throw std::runtime_error("Failed to open WAL file: " + path_.string());
  }
}

void Wal::close_file() {
  if (fd_ != -1) {
    ::close(fd_);
    fd_ = -1;
  }
}

void Wal::write(std::string_view message) {
  const char *data = message.data();
  size_t remaining = message.size();

  while (remaining > 0) {
    ssize_t written = ::write(fd_, data, remaining);
    if (written == -1) {
      throw std::runtime_error("Failed to write to WAL");
    }
    data += written;
    remaining -= written;
  }

  // Ensure data reaches disk
  sync();
}

void Wal::sync() {
  if (::fsync(fd_) == -1) {
    throw std::runtime_error("Failed to sync WAL to disk");
  }
}

void Wal::clear() {
  // Truncate file to zero bytes
  if (::ftruncate(fd_, 0) == -1) {
    throw std::runtime_error("Failed to truncate WAL");
  }
  // With O_APPEND, the write position automatically goes to end (which is now 0)
}

} // namespace lsm_storage_engine
