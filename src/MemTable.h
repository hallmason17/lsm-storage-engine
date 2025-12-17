#pragma once
#include <filesystem>
namespace lsm_storage_engine {
class MemTable {
public:
  void restore_from_wal(const std::filesystem::path &wal_path);

private:
};
} // namespace lsm_storage_engine
