#pragma once
#include "StorageError.h"
#include <expected>
#include <filesystem>
#include <optional>
#include <stdexcept>
namespace lsm_storage_engine {

/**
 * @brief Immutable on-disk sorted string table.
 *
 * SSTable (Sorted String Table) stores key-value pairs in sorted order on
 * disk. Each SSTable is created by flushing a MemTable and is immutable once
 * written. Keys are stored in lexicographic order to support efficient lookups
 * and range scans.
 *
 * Read operations are thread-safe. The table is immutable after creation.
 */
class SSTable {
public:
  /**
   * @brief Creates a new SSTable with a generated filename.
   *
   * The filename is the current timestamp to ensure uniqueness.
   * @throws std::runtime_error if the file cannot be opened/created.
   */
  SSTable() {
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    path_ = std::to_string(now.count()) + ".sst";
    if (!open_file()) {
      throw std::runtime_error("Unable to open SSTable!");
    }
  }

  /**
   * @brief Opens an existing SSTable from the specified path.
   * @param path Path to the SSTable file.
   * @throws std::runtime_error if the file cannot be opened.
   */
  SSTable(std::filesystem::path path) : path_(std::move(path)) {
    if (!open_file()) {
      throw std::runtime_error("Unable to open SSTable!");
    }
  }

  ~SSTable() { close_file(); }

  // No copies.
  SSTable(const SSTable &) = delete;
  SSTable &operator=(const SSTable &) = delete;

  // Moves OK.
  SSTable(SSTable &&other) noexcept;
  SSTable &operator=(SSTable &&other) noexcept;

  /**
   * @brief Returns the file path of this SSTable.
   */
  const std::filesystem::path &path() const { return path_; }

  /**
   * @brief Searches for a key in the SSTable.
   * @param key The key to look up.
   * @return The value if found, std::nullopt if not found, or StorageError
   *         on I/O failure.
   */
  std::expected<std::optional<std::string>, StorageError>
  get(std::string_view key) const;

private:
  std::filesystem::path path_;
  int fd_{-1};
  // TODO: Add a refcount

  /**
   * @brief Opens the SSTable file for reading.
   * @return void on success, StorageError on failure.
   */
  std::expected<void, StorageError> open_file();

  /**
   * @brief Closes the file descriptor if open.
   */
  void close_file();
};
} // namespace lsm_storage_engine
