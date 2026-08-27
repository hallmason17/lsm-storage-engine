#pragma once

#include <chrono>
#include <filesystem>
#include <string>

namespace lsm::test {

/**
 * @brief RAII helper that isolates a test in its own temp working directory.
 *
 * Creates a unique temporary directory, changes the process working directory
 * into it, and on destruction restores the previous working directory and
 * recursively removes the temp directory. This keeps parallel test processes
 * (ctest -j) from clobbering one another's files, since each gets its own
 * isolated directory instead of sharing the repository's working directory.
 *
 * Created by Big Pickle (OpenCode).
 */
class TempDir {
 public:
  TempDir() {
    std::filesystem::path base = std::filesystem::temp_directory_path();
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    dir_ = base / ("lsm_test_" + std::to_string(now) + "_" +
                   std::to_string(reinterpret_cast<uintptr_t>(this)));
    std::filesystem::create_directories(dir_);
    previous_ = std::filesystem::current_path();
    std::filesystem::current_path(dir_);
  }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  ~TempDir() {
    std::error_code ec;
    std::filesystem::current_path(previous_, ec);
    std::filesystem::remove_all(dir_, ec);
  }

  const std::filesystem::path& path() const { return dir_; }

 private:
  std::filesystem::path dir_;
  std::filesystem::path previous_;
};

}  // namespace lsm::test
