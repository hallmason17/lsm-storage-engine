#pragma once
#include <filesystem>
#include <fstream>
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
  explicit Wal(std::filesystem::path filename)
      : path_{std::move(filename)}, file_{path_, std::ios::app} {}

  /// Managing file handles, so no copies.
  Wal(const Wal &) = delete;
  Wal &operator=(const Wal &) = delete;

  /// Moving resources OK.
  Wal(Wal &&) = default;
  Wal &operator=(Wal &&) = default;

  /**
   * @brief Write a message to the log
   * @param message The message to append to the log
   */
  void write(const std::string &message);

  /**
   * @brief Get the path to the WAL.
   * @returns The path where the log is located
   */
  const std::filesystem::path &path() const { return path_; }

private:
  std::filesystem::path path_;

  /**
   * This is quick and hacky; just a placeholder for now.
   *
   * ofstream does not immediately flush to disk, but goes to an in-memory
   * buffer first. Not robust enough for a storage engine.
   *
   * TODO: replace with fd and use syscalls for interacting.
   */
  std::ofstream file_;
};
} // namespace lsm_storage_engine
