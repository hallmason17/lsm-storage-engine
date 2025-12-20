#include "LsmTree.h"
#include "MemTable.h"
#include <chrono>
#include <filesystem>
#include <format>
#include <fstream>
#include <optional>
#include <print>
#include <ranges>
#include <shared_mutex>
#include <stdexcept>
#include <unistd.h>
namespace lsm_storage_engine {
std::optional<std::string> LsmTree::get(const std::string_view key) {
  auto start = std::chrono::high_resolution_clock::now();

  std::optional<std::string> result;
  {
    // Reads don't block other reads, but block writes.
    std::shared_lock lock(rwlock_);
    if (auto val = mem_table_.get(key)) {
      result = *val;
    } else {
      for (const auto &sst : ss_tables_ | std::views::reverse) {
        auto res = sst.get(key);

        // Check the expected and the optional!!!
        if (res && res->has_value()) {
          result = res.value();
          break;
        }
      }
    }
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  total_get_time_us_.fetch_add(duration_us, std::memory_order_relaxed);
  get_count_.fetch_add(1, std::memory_order_relaxed);

  return result;
}
void LsmTree::put(const std::string &key, const std::string &value) {
  auto start = std::chrono::high_resolution_clock::now();

  {
    std::string msg = std::format("p {} {}\n", key, value);

    // Lock to ensure these two operations are atomic.
    std::unique_lock lock(rwlock_);
    if (!wal_.write(msg)) {
      throw std::runtime_error("Failed to write to WAL!");
    }
    mem_table_.put(key, value);
    if (mem_table_.should_flush()) {
      SSTable sst = SSTable::create().value();
      auto res = update_meta(sst);
      if (!res) {
        throw std::runtime_error("Could not add SSTable to meta file!");
      }
      if (!mem_table_.flush_to_disk(sst.path())) {
        throw std::runtime_error("Failed to write memtable to disk!");
      }
      mem_table_.clear();
      if (!wal_.clear()) {
        throw std::runtime_error("Could not empty the WAL!");
      }
      ss_tables_.push_back(std::move(sst));
    }
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  total_put_time_us_.fetch_add(duration_us, std::memory_order_relaxed);
  put_count_.fetch_add(1, std::memory_order_relaxed);
}
std::expected<void, StorageError> LsmTree::load_ssts() {
  if (std::filesystem::exists("lsm.meta")) {
    std::ifstream metafile{"lsm.meta"};
    std::string line;
    while (std::getline(metafile, line)) {
      if (line.contains(".sst")) {
        std::println(stderr, "Meta file line: {}", line);
        SSTable sst = SSTable::open(std::string(line)).value();
        ss_tables_.emplace_back(std::move(sst));
      }
    }
  }
  return {};
}

LsmTree::Stats LsmTree::stats() const {
  auto get_count = get_count_.load(std::memory_order_relaxed);
  auto put_count = put_count_.load(std::memory_order_relaxed);
  auto total_get_us = total_get_time_us_.load(std::memory_order_relaxed);
  auto total_put_us = total_put_time_us_.load(std::memory_order_relaxed);

  return Stats{
      .get_count = get_count,
      .put_count = put_count,
      .avg_get_time_us =
          get_count > 0 ? static_cast<double>(total_get_us) / get_count : 0.0,
      .avg_put_time_us =
          put_count > 0 ? static_cast<double>(total_put_us) / put_count : 0.0,
  };
}
std::expected<void, StorageError> LsmTree::update_meta(SSTable &sstable) {
  std::ofstream metafile("lsm.meta", std::ios::app);
  if (!metafile.is_open()) {
    return std::unexpected(StorageError::file_open("lsm.meta"));
  }
  metafile << sstable.path().filename().string() << '\n';
  if (!metafile.good()) {
    return std::unexpected(StorageError::file_write("lsm.meta"));
  }
  return {};
}
} // namespace lsm_storage_engine
