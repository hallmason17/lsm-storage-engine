#include "SSTable.h"
#include <expected>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
namespace lsm_storage_engine {

SSTable::SSTable(SSTable &&other) noexcept
    : path_{std::move(other.path_)}, fd_{std::exchange(other.fd_, -1)} {}

SSTable &SSTable::operator=(SSTable &&other) noexcept {
  if (this != &other) {
    close_file();
    path_ = std::move(other.path_);
    fd_ = std::exchange(other.fd_, -1);
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
}

namespace {
enum class ReadStatus { Ok, Eof };

std::expected<ReadStatus, StorageError>
read_exact(int fd, void *buf, size_t len, const std::filesystem::path &path) {
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
SSTable::get(std::string_view key) const {
  ::lseek(fd_, 0, SEEK_SET);
  uint32_t keylen{0};
  uint32_t valuelen{0};

  while (true) {
    auto status = read_exact(fd_, &keylen, sizeof(keylen), path_);
    if (!status)
      return std::unexpected(status.error());
    if (*status == ReadStatus::Eof)
      return std::nullopt;

    auto _ = read_exact(fd_, &valuelen, sizeof(valuelen), path_);

    std::string k(keylen, '\0');
    std::string val(valuelen, '\0');
    _ = read_exact(fd_, k.data(), keylen, path_);
    _ = read_exact(fd_, val.data(), valuelen, path_);

    if (k == key)
      return val;
  }
}
} // namespace lsm_storage_engine
