#pragma once
#include "StorageError.h"
#include <expected>
#include <filesystem>
#include <optional>
#include <stdexcept>
namespace lsm_storage_engine {
class SSTable {
public:
  SSTable() {
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    path_ = std::to_string(now.count()) + ".sst";
    if (!open_file()) {
      throw std::runtime_error("Unable to open SSTable!");
    }
  }
  SSTable(std::filesystem::path path) : path_(std::move(path)) {
    if (!open_file()) {
      throw std::runtime_error("Unable to open SSTable!");
    }
  }
  ~SSTable() { close_file(); }
  SSTable(const SSTable &) = delete;
  SSTable &operator=(const SSTable &) = delete;
  SSTable(SSTable &&other) noexcept;
  SSTable &operator=(SSTable &&other) noexcept;

  const std::filesystem::path &path() const { return path_; }

  std::expected<std::optional<std::string>, StorageError>
  get(std::string_view key) const;

private:
  std::filesystem::path path_;
  int fd_{-1};

  std::expected<void, StorageError> open_file();
  void close_file();
};
} // namespace lsm_storage_engine
