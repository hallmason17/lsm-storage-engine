#include "LsmTree.h"
#include "MemTable.h"
#include "SSTable.h"
#include "StorageError.h"
#include <algorithm>
#include <chrono>
#include <expected>
#include <filesystem>
#include <format>
#include <fstream>
#include <optional>
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

std::expected<void, StorageError> LsmTree::flush_memtable() {
  SSTable sst = SSTable::create().value();
  auto update_result = update_meta(sst);
  if (!update_result) {
    return std::unexpected(StorageError::file_write("lsm.meta"));
  }
  if (!mem_table_.flush_to_disk(sst.path())) {
    return std::unexpected(StorageError::file_write(sst.path()));
  }
  mem_table_.clear();
  if (!wal_.clear()) {
    return std::unexpected(StorageError::file_write(wal_.path()));
  }
  ss_tables_.push_back(std::move(sst));
  return {};
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
      auto flush_result = flush_memtable();
      if (!flush_result) {
        throw std::runtime_error(
            "Failed to create SST! Error: " + flush_result.error().message +
            " " + flush_result.error().path.string());
      }
    }
    auto compact_result = maybe_compact();
    if (!compact_result) {
      throw std::runtime_error(
          "Failed to compact SSTs: " + compact_result.error().message + ": " +
          compact_result.error().path.string());
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

void LsmTree::write_sst_entry(
    std::ofstream &out, const std::pair<std::string, std::string> &entry) {
  auto keylen = static_cast<uint32_t>(entry.first.size());
  auto valuelen = static_cast<uint32_t>(entry.second.size());

  out.write(reinterpret_cast<const char *>(&keylen), sizeof(keylen));
  out.write(reinterpret_cast<const char *>(&valuelen), sizeof(valuelen));
  out.write(entry.first.data(),
            static_cast<std::streamsize>(entry.first.size()));
  out.write(entry.second.data(),
            static_cast<std::streamsize>(entry.second.size()));
}

static void cleanup_sst_files(std::vector<SSTable> &ss_tables) {
  for (auto &sst : ss_tables) {
    if (sst.marked_for_delete_) {
      std::filesystem::remove(sst.path());
    }
  }
}

std::expected<void, StorageError> LsmTree::maybe_compact() {
  // some random number for testing
  // TODO: come up with a real compaction trigger
  if (ss_tables_.size() < 4) {
    return {};
  }
  std::vector<SSTable> new_ssts;
  for (size_t i = 0; i + 1 < ss_tables_.size(); i += 2) {
    // Make a new sst
    auto sst = SSTable::create();
    if (!sst) {
      return std::unexpected(
          StorageError::file_open("failed to create SSTable"));
    }
    std::ofstream of(sst->path(), std::ios::binary | std::ios::app);
    if (!of.good()) {
      return std::unexpected(StorageError::file_open(sst->path()));
    }
    auto lhs = ss_tables_[i].next();
    auto rhs = ss_tables_[i + 1].next();
    // merge sort combine
    while (lhs->has_value() || rhs->has_value()) {
      if (!lhs->has_value() && rhs->has_value()) {
        write_sst_entry(of, rhs->value());
        rhs = ss_tables_[i + 1].next();
      } else if (!rhs->has_value() && lhs->has_value()) {
        write_sst_entry(of, lhs->value());
        lhs = ss_tables_[i].next();
      } else if (lhs->value().first < rhs->value().first) {
        write_sst_entry(of, lhs->value());
        lhs = ss_tables_[i].next();
      } else if (rhs->value().first < lhs->value().first) {
        write_sst_entry(of, rhs->value());
        rhs = ss_tables_[i + 1].next();
      } else {
        // Keys are equal - keep the newer value (rhs)
        write_sst_entry(of, rhs->value());
        lhs = ss_tables_[i].next();
        rhs = ss_tables_[i + 1].next();
      }
    }
    ss_tables_[i].marked_for_delete_ = true;
    ss_tables_[i + 1].marked_for_delete_ = true;
    new_ssts.push_back(std::move(sst.value()));
  }
  cleanup_sst_files(ss_tables_);
  ss_tables_ = std::move(new_ssts);

  std::filesystem::resize_file("lsm.meta", 0);

  for (auto &sst : ss_tables_) {
    auto res = update_meta(sst);
    if (!res) {
      return std::unexpected(res.error());
    }
  }
  return {};
}
} // namespace lsm_storage_engine
