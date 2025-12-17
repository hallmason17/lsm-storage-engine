#pragma once
#include <filesystem>
#include <fstream>
namespace lsm_storage_engine {
class Wal {
public:
  explicit Wal(std::filesystem::path filename)
      : path_{std::move(filename)}, file_{path_, std::ios::app} {}

  void write(const std::string &message);
  const std::filesystem::path &path() const { return path_; }

private:
  std::filesystem::path path_;
  std::ofstream file_;
};
} // namespace lsm_storage_engine
