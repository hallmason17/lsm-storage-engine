#include "LsmTree.h"

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <expected>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <queue>
#include <ranges>
#include <shared_mutex>
#include <stdexcept>

#include "BloomFilter.h"
#include "MemTable.h"
#include "SSTable.h"
#include "StorageError.h"
namespace lsm {
std::optional<std::string> LsmTree::get(const std::string_view key) {
  auto start = std::chrono::high_resolution_clock::now();

  std::optional<std::string> result;
  {
    // Reads don't block other reads, but block writes.
    std::shared_lock lock(rwlock_);
    if (auto val = mem_table_.get(key)) {
      result = *val;
    } else {
      for (auto& sst : ss_tables_ | std::views::reverse) {
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
  if (auto res = wal_.fsync(); !res) {
    return res;
  }
  auto result =
      SSTable::create(database_name_.string())
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
void LsmTree::put(const std::string& key, const std::string& value, bool sync) {
  auto start = std::chrono::high_resolution_clock::now();

  {
    // Lock to ensure these two operations are atomic.
    std::unique_lock lock(rwlock_);
    if (!wal_.write(key, value, sync)) {
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
  if (std::filesystem::exists(metadata_file_name_)) {
    std::ifstream metafile{metadata_file_name_};
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
std::expected<void, StorageError> LsmTree::update_meta(SSTable& sstable) {
  std::ofstream metafile(metadata_file_name_, std::ios::app);
  if (!metafile.is_open()) {
    return std::unexpected(StorageError::file_open(metadata_file_name_));
  }
  metafile << sstable.path().filename().string() << '\n';
  if (!metafile.good()) {
    return std::unexpected(StorageError::file_write(metadata_file_name_));
  }
  return {};
}

static void cleanup_sst_files(std::vector<SSTable>& ss_tables) {
  for (auto& sst : ss_tables) {
    if (sst.marked_for_delete_) {
      std::filesystem::remove(sst.path());
    }
  }
}

std::expected<void, StorageError> LsmTree::maybe_compact() {
  if (ss_tables_.size() < 4) {
    return {};
  }
  std::vector<SSTable> new_ssts;
  auto new_sst = SSTable::create(database_name_.string());
  if (!new_sst) {
    return std::unexpected(new_sst.error());
  }

  // BloomFilter(n) allocates n * 10 bits — recover entry counts from that.
  size_t num_entries{0};
  std::string min_key, max_key;
  for (auto& sst : ss_tables_) {
    auto bloom = sst.read_bloom_filter();
    if (!bloom) {
      return std::unexpected(bloom.error());
    }
    num_entries += bloom.value().bits().size() / 10;

    if (min_key.empty() || sst.header().min_key < min_key) {
      min_key = sst.header().min_key;
    }
    if (max_key.empty() || sst.header().max_key > max_key) {
      max_key = sst.header().max_key;
    }
  }

  // First pass: populate bloom
  BloomFilter new_bloom{num_entries};
  for (auto& sst : ss_tables_) {
    sst.rewind();
    while (true) {
      auto entry = sst.next();
      if (!entry) {
        return std::unexpected(entry.error());
      }
      if (!entry->has_value()) {
        break;
      }
      new_bloom.add(entry->value().first);
    }
  }

  SSTable::Header header{min_key, max_key};
  if (auto res = new_sst.value().write_header(std::move(header)); !res) {
    return std::unexpected{res.error()};
  }
  size_t bytes_written{new_sst->header().size};

  auto bf_res = new_sst->write_bloom_filter(std::move(new_bloom));
  if (!bf_res) {
    return std::unexpected{bf_res.error()};
  }
  bytes_written += bf_res.value();

  // Second pass: k-way merge into blocks.
  size_t k = ss_tables_.size();

  struct merge_helper {
    std::optional<std::pair<std::string, std::string>> kv;
    size_t index;
    SSTable* sst;
  };

  auto merge_comparator = [](const merge_helper& a, const merge_helper& b) {
    if (a.kv->first != b.kv->first) return a.kv->first > b.kv->first;
    return a.index < b.index;
  };
  std::priority_queue<merge_helper, std::vector<merge_helper>,
                      decltype(merge_comparator)>
      queue{merge_comparator};

  // Load with first value from each SSTable.
  for (size_t i = 0; i < k; ++i) {
    ss_tables_[i].rewind();
    auto entry = ss_tables_[i].next();
    if (!entry) {
      return std::unexpected(entry.error());
    }
    if (entry->has_value()) {
      queue.push({std::move(entry.value()), i, &ss_tables_[i]});
    }
  }

  // Block as the temporary buffer.
  Block block;
  std::optional<merge_helper> prev = std::nullopt;
  while (!queue.empty()) {
    auto top = queue.top();
    queue.pop();
    if (!top.kv.has_value()) {
      continue;
    }
    if (prev != std::nullopt && top.kv->first == prev->kv->first) {
      continue;
    }

    if (block.append(top.kv->first, top.kv->second) == 0) {
      // Block full, save it to the new SSTable and make a new block.
      new_sst->index().emplace_back(std::string(block.first().value()),
                                    bytes_written);
      auto write_res = new_sst->write_block(block);
      if (!write_res) {
        return std::unexpected{write_res.error()};
      }
      bytes_written += write_res.value();
      block = Block{};
      if (block.append(top.kv->first, top.kv->second) == 0) {
        return std::unexpected(StorageError::file_write(new_sst->path()));
      }
    }

    // Advance the iterator for this SSTable, reinsert into the queue.
    auto next = top.sst->next();
    if (!next) {
      return std::unexpected(next.error());
    }
    if (next->has_value()) {
      queue.push({std::move(next.value()), top.index, top.sst});
    }
    prev = top;
  }

  // Write leftover to the new SSTable.
  if (block.size() > 0) {
    new_sst->index().emplace_back(std::string(block.first().value()),
                                  bytes_written);
    auto write_res = new_sst->write_block(block);
    if (!write_res) {
      return std::unexpected{write_res.error()};
    }
    bytes_written += write_res.value();
  }

  for (auto& sst : ss_tables_) {
    sst.marked_for_delete_ = true;
  }

  SSTable::Footer footer;
  footer.index_offset = bytes_written;
  auto idx_res = new_sst->write_index();
  if (!idx_res) {
    return std::unexpected{idx_res.error()};
  }
  footer.index_size = idx_res.value();
  footer.num_index_entries = new_sst->index().size();

  if (auto res = new_sst.value().write_footer(footer); !res) {
    return std::unexpected{res.error()};
  }
  new_ssts.push_back(std::move(new_sst.value()));
  cleanup_sst_files(ss_tables_);
  ss_tables_ = std::move(new_ssts);

  std::filesystem::resize_file(metadata_file_name_, 0);

  for (auto& sst : ss_tables_) {
    auto res = update_meta(sst);
    if (!res) {
      return std::unexpected(res.error());
    }
  }

  return {};
}
std::expected<void, StorageError> LsmTree::maybe_compact1() {
  // some random number for testing
  // TODO: come up with a real compaction trigger
  if (ss_tables_.size() < 4) {
    return {};
  }
  std::vector<SSTable> new_ssts;
  for (size_t i = 0; i + 1 < ss_tables_.size(); i += 2) {
    // Make a new sst
    auto sst = SSTable::create(database_name_.string());
    if (!sst) {
      return std::unexpected(sst.error());
    }
    SSTable& left_table = ss_tables_[i];
    SSTable& right_table = ss_tables_[i + 1];
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

    // First pass: collect all keys for the bloom filter and count entries
    left_table.rewind();
    right_table.rewind();

    std::vector<std::pair<std::string, std::string>> all_entries;
    auto lhs = left_table.next();
    auto rhs = right_table.next();
    while ((lhs && rhs) && (lhs->has_value() || rhs->has_value())) {
      if (!lhs->has_value() && rhs->has_value()) {
        all_entries.emplace_back(rhs->value());
        rhs = right_table.next();
      } else if (!rhs->has_value() && lhs->has_value()) {
        all_entries.emplace_back(lhs->value());
        lhs = left_table.next();
      } else if (lhs->value().first < rhs->value().first) {
        all_entries.emplace_back(lhs->value());
        lhs = left_table.next();
      } else if (rhs->value().first < lhs->value().first) {
        all_entries.emplace_back(rhs->value());
        rhs = right_table.next();
      } else {
        // Keys are equal - keep the newer value (rhs)
        all_entries.emplace_back(rhs->value());
        lhs = left_table.next();
        rhs = right_table.next();
      }
    }

    size_t bf_size = all_entries.size();
    BloomFilter bloom_filter{bf_size};
    for (const auto& [key, val] : all_entries) {
      bloom_filter.add(std::string_view{key});
    }
    auto bf_res = sst->write_bloom_filter(std::move(bloom_filter));
    if (!bf_res) {
      return std::unexpected{bf_res.error()};
    }
    bytes_written += bf_res.value();

    // Second pass: write all entries as blocks
    Block block;
    for (const auto& [key, val] : all_entries) {
      if (block.append(key, val) == 0) {
        sst->index().emplace_back(std::string(block.first().value()),
                                  bytes_written);
        auto write_res = sst->write_block(block);
        if (!write_res) {
          return std::unexpected{write_res.error()};
        }
        bytes_written += write_res.value();
        block = Block{};
        if (block.append(key, val) == 0) {
          return std::unexpected(StorageError::file_write(sst->path()));
        }
      }
    }
    if (block.size() > 0) {
      sst->index().emplace_back(std::string(block.first().value()),
                                bytes_written);
      auto write_res = sst->write_block(block);
      if (!write_res) {
        return std::unexpected{write_res.error()};
      }
      bytes_written += write_res.value();
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

  std::filesystem::resize_file(metadata_file_name_, 0);

  for (auto& sst : ss_tables_) {
    auto res = update_meta(sst);
    if (!res) {
      return std::unexpected(res.error());
    }
  }
  return {};
}

// TODO
void LsmTree::rm(const std::string&) {}
}  // namespace lsm
