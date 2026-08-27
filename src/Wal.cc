#include "Wal.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <expected>
#include <mutex>
#include <print>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <utility>
#include <vector>

#include "StorageError.h"
#include "utils/CheckSum.h"

namespace lsm {

Wal::Wal(std::filesystem::path filename) : path_{std::move(filename)} {
  if (!open_file()) {
    throw std::runtime_error("Unable to open WAL");
  }
  fsync_thread = std::jthread([&](std::stop_token tok) { fsync_worker(tok); });
}
void Wal::fsync_worker(std::stop_token tok) {
  std::condition_variable_any cond;
  std::mutex mu;
  while (!tok.stop_requested()) {
    if (auto to_sync = unsynced_.exchange(0, std::memory_order_relaxed);
        to_sync > 0) {
      if (auto res = fsync(); !res) {
        unsynced_.fetch_add(to_sync, std::memory_order_relaxed);
        std::println(stderr, "{}", res.error().message);
      }
      std::this_thread::yield();
      continue;
    }

    std::unique_lock lk(mu);
    cond.wait_for(lk, tok, std::chrono::milliseconds(250),
                  [&] { return tok.stop_requested(); });
  }
}

Wal::~Wal() {
  fsync_thread.request_stop();
  fsync_thread.join();
  close_file();
}

Wal::Wal(Wal&& other) noexcept
    : path_{std::move(other.path_)},
      fd_{std::exchange(other.fd_, -1)},
      unsynced_{other.unsynced_.exchange(0)},
      fsync_thread(std::move(other.fsync_thread)) {}

Wal& Wal::operator=(Wal&& other) noexcept {
  if (this != &other) {
    fsync_thread.request_stop();
    fsync_thread.join();
    close_file();
    path_ = std::move(other.path_);
    fd_ = std::exchange(other.fd_, -1);
    unsynced_.exchange(other.unsynced_);
    fsync_thread = std::move(other.fsync_thread);
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
                                             std::string_view value,
                                             bool sync) const {
  std::vector<std::byte> write_buffer;
  auto keylen = static_cast<uint32_t>(key.size());
  auto valuelen = static_cast<uint32_t>(value.size());

  auto append = [&write_buffer](const void* d, size_t len) {
    auto data = reinterpret_cast<const std::byte*>(d);
    write_buffer.insert(write_buffer.end(), data, data + len);
  };

  append(&keylen, sizeof(keylen));
  append(&valuelen, sizeof(valuelen));
  append(key.data(), key.size());
  append(value.data(), value.size());

  auto cs = hash32({reinterpret_cast<const char*>(write_buffer.data()),
                    write_buffer.size()});

  append(&cs, sizeof(cs));

  assert(fd_ > -1);

  size_t remaining = write_buffer.size();
  ssize_t written = 0;
  while (remaining > 0) {
    auto write_res = ::write(fd_, write_buffer.data() + written, remaining);
    if (write_res == -1) {
      return std::unexpected{StorageError::file_write(path())};
    }
    written += write_res;
    remaining -= static_cast<size_t>(write_res);
  }
  unsynced_.fetch_add(write_buffer.size());
  if (sync) {
    return fsync();
  }

  return {};
}

std::expected<void, StorageError> Wal::fsync() const {
  auto unsynced = unsynced_.exchange(0, std::memory_order_relaxed);
  if (unsynced == 0) {
    return {};
  }
  if (::fdatasync(fd_) == -1) {
    unsynced_.fetch_add(unsynced, std::memory_order_relaxed);
    return std::unexpected{StorageError::file_write(path())};
  }
  return {};
}

std::expected<void, StorageError> Wal::clear() const {
  if (::ftruncate(fd_, 0) == -1) {
    return std::unexpected{StorageError::file_write(path())};
  }
  unsynced_.exchange(0, std::memory_order_relaxed);
  return {};
}

}  // namespace lsm
