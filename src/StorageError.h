#pragma once
#include <filesystem>
#include <string>
namespace lsm_storage_engine {
struct StorageError {
  enum class Kind { FileOpen, FileWrite, FileRead, Corruption };
  Kind kind;
  std::string message;
  std::filesystem::path path;

  static StorageError file_open(const std::filesystem::path &path) {
    return {Kind::FileOpen, "Failed to open file", path};
  }

  static StorageError file_write(const std::filesystem::path &path) {
    return {Kind::FileWrite, "Could not write to file", path};
  }

  static StorageError file_read(const std::filesystem::path &path) {
    return {Kind::FileRead, "Failed to read file", path};
  }
};
} // namespace lsm_storage_engine
