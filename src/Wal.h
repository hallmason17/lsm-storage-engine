#pragma once
#include <filesystem>
#include <string_view>
namespace lsm_storage_engine {

/**
 * @brief Implementation for the Write-Ahead Log.
 *
 * Writes from users come here first, followed by the memtable.
 * Once a command is written to this log, it becomes durable. Upon a crash, the
 * memtable will load everything in this log.
 */
class Wal {
public:
  explicit Wal(std::filesystem::path filename);
  ~Wal();

  /// Managing file handles, so no copies.
  Wal(const Wal &) = delete;
  Wal &operator=(const Wal &) = delete;

  /// Moving resources OK.
  Wal(Wal &&other) noexcept;
  Wal &operator=(Wal &&other) noexcept;

  /**
   * @brief Write a message to the log and sync to disk.
   * @param message The message to append to the log
   */
  void write(std::string_view message);

  /**
   * @brief Get the path to the WAL.
   * @returns The path where the log is located
   */
  const std::filesystem::path &path() const { return path_; }

  /**
   * @brief Truncate the WAL to zero bytes.
   */
  void clear();

  /**
   * @brief Sync buffered writes to disk.
   */
  void sync();

private:
  std::filesystem::path path_;
  int fd_{-1};

  void open_file();
  void close_file();
};
} // namespace lsm_storage_engine
