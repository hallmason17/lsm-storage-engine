#include "LsmTree.h"
#include "Constants.h"
#include "MemTable.h"
#include "SSTable.h"
#include "StorageError.h"
#include "utils/CheckSum.h"
#include <atomic>
#include <chrono>
#include <expected>
#include <filesystem>
#include <fstream>
#include <mutex>
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
      for (auto &sst : ss_tables_ | std::views::reverse) {
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
  auto max = max_get_time_us_.load(std::memory_order_relaxed);
  while (duration_us > max) {
    if (max_get_time_us_.compare_exchange_weak(max, duration_us,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed))
      break;
  }

  return result;
}

std::expected<void, StorageError> LsmTree::flush_memtable() {
  auto result =
      SSTable::create()
          .and_then([&](SSTable sst) {
            return update_meta(sst).transform([&] { return std::move(sst); });
          })
          .and_then([&](SSTable sst) -> std::expected<SSTable, StorageError> {
            if (!mem_table_.flush_to_sst(sst)) {
              return std::unexpected(StorageError::file_write(sst.path()));
            }
            return sst;
          })
          .and_then([&](SSTable sst) -> std::expected<void, StorageError> {
            mem_table_.clear();
            if (!wal_.clear()) {
              return std::unexpected(StorageError::file_write(wal_.path()));
            }
            ss_tables_.push_back(std::move(sst));
            return {};
          });
  return result;
}
void LsmTree::put(const std::string &key, const std::string &value) {
  auto start = std::chrono::high_resolution_clock::now();

  {

    // Lock to ensure these two operations are atomic.
    std::unique_lock lock(rwlock_);
    if (!wal_.write(key, value)) {
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
  auto max = max_put_time_us_.load(std::memory_order_relaxed);
  while (duration_us > max) {
    if (max_put_time_us_.compare_exchange_weak(max, duration_us,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed))
      break;
  }
}
std::expected<void, StorageError> LsmTree::load_ssts() {
  if (std::filesystem::exists("lsm.meta")) {
    std::ifstream metafile{"lsm.meta"};
    std::string line;
    while (std::getline(metafile, line)) {
      if (line.contains(".sst")) {
        auto result =
            SSTable::open(std::string(line))
                .and_then(
                    [&](SSTable table) -> std::expected<void, StorageError> {
                      ss_tables_.emplace_back(std::move(table));
                      return {};
                    });
        if (!result) {
          return std::unexpected{result.error()};
        }
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
  auto max_get_us = max_get_time_us_.load(std::memory_order_relaxed);
  auto max_put_us = max_put_time_us_.load(std::memory_order_relaxed);

  return Stats{
      .get_count = get_count,
      .put_count = put_count,
      .avg_get_time_us = get_count > 0 ? static_cast<double>(total_get_us) /
                                             static_cast<double>(get_count)
                                       : 0.0,
      .avg_put_time_us = put_count > 0 ? static_cast<double>(total_put_us) /
                                             static_cast<double>(put_count)
                                       : 0.0,
      .max_put_time_us_ = max_put_us,
      .max_get_time_us_ = max_get_us,
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
  if (ss_tables_.size() < 12) {
    return {};
  }
  std::vector<SSTable> new_ssts;
  for (size_t i = 0; i + 1 < ss_tables_.size(); i += 2) {
    // Make a new sst
    auto sst = SSTable::create();
    if (!sst) {
      return std::unexpected(sst.error());
    }
    SSTable &left_table = ss_tables_[i];
    SSTable &right_table = ss_tables_[i + 1];
    auto min_key = left_table.header().min_key < right_table.header().min_key
                       ? left_table.header().min_key
                       : right_table.header().min_key;
    auto max_key = left_table.header().max_key > right_table.header().max_key
                       ? left_table.header().max_key
                       : right_table.header().max_key;
    SSTable::Header header{min_key, max_key};
    if (auto res = sst.value().write_header(std::move(header)); !res) {
      return std::unexpected{res.error()};
    }
    size_t bytes_written{sst->header().size};
    size_t entry_count{0};
    auto lhs = left_table.next();
    auto rhs = right_table.next();
    // merge sort combine
    while ((lhs && rhs) && (lhs->has_value() || rhs->has_value())) {
      if (!lhs->has_value() && rhs->has_value()) {
        auto write_res =
            sst->write_entry(rhs->value().first, rhs->value().second);
        if (!write_res) {
          return std::unexpected{write_res.error()};
        }
        if (entry_count % lsm_constants::kIndexSpace == 0) {
          sst->index().emplace_back(std::string{rhs->value().first},
                                    bytes_written);
        }
        bytes_written += write_res.value();
        entry_count++;
        rhs = ss_tables_[i + 1].next();
      } else if (!rhs->has_value() && lhs->has_value()) {
        auto write_res =
            sst->write_entry(lhs->value().first, lhs->value().second);
        if (!write_res) {
          return std::unexpected{write_res.error()};
        }
        if (entry_count % lsm_constants::kIndexSpace == 0) {
          sst->index().emplace_back(std::string{lhs->value().first},
                                    bytes_written);
        }
        bytes_written += write_res.value();
        entry_count++;
        lhs = ss_tables_[i].next();
      } else if (lhs->value().first < rhs->value().first) {
        auto write_res =
            sst->write_entry(lhs->value().first, lhs->value().second);
        if (!write_res) {
          return std::unexpected{write_res.error()};
        }
        if (entry_count % lsm_constants::kIndexSpace == 0) {
          sst->index().emplace_back(std::string{lhs->value().first},
                                    bytes_written);
        }
        bytes_written += write_res.value();
        entry_count++;
        lhs = ss_tables_[i].next();
      } else if (rhs->value().first < lhs->value().first) {
        auto write_res =
            sst->write_entry(rhs->value().first, rhs->value().second);
        if (!write_res) {
          return std::unexpected{write_res.error()};
        }
        if (entry_count % lsm_constants::kIndexSpace == 0) {
          sst->index().emplace_back(std::string{rhs->value().first},
                                    bytes_written);
        }
        bytes_written += write_res.value();
        entry_count++;
        rhs = ss_tables_[i + 1].next();
      } else {
        // Keys are equal - keep the newer value (rhs)
        auto write_res =
            sst->write_entry(rhs->value().first, rhs->value().second);
        if (!write_res) {
          return std::unexpected{write_res.error()};
        }
        if (entry_count % lsm_constants::kIndexSpace == 0) {
          sst->index().emplace_back(std::string{rhs->value().first},
                                    bytes_written);
        }
        bytes_written += write_res.value();
        entry_count++;
        lhs = ss_tables_[i].next();
        rhs = ss_tables_[i + 1].next();
      }
    }
    left_table.marked_for_delete_ = true;
    right_table.marked_for_delete_ = true;

    SSTable::Footer footer;
    footer.index_offset = bytes_written;
    auto idx_res = sst->write_index();
    if (!idx_res) {
      return std::unexpected{idx_res.error()};
    }
    footer.index_size = idx_res.value();
    footer.num_index_entries = sst->index().size();

    if (auto res = sst.value().write_footer(footer); !res) {
      return std::unexpected{res.error()};
    }
    new_ssts.push_back(std::move(sst.value()));
  }
  if (ss_tables_.size() % 2 == 1) {
    new_ssts.push_back(std::move(ss_tables_.back()));
    ss_tables_.pop_back();
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
void LsmTree::rm(const std::string &) {}
} // namespace lsm_storage_engine
