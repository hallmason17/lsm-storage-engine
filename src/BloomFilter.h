#pragma once
#include <cmath>
#include <vector>
namespace lsm_storage_engine {
class BloomFilter {
public:
  BloomFilter() {}
  BloomFilter(size_t num_items) : bits_(num_items * 10, false) {}
  BloomFilter(std::vector<bool> bits) : bits_{std::move(bits)} {}

  void add(const std::string_view key);
  bool contains(const std::string_view key);
  const std::vector<bool> &bits() const { return bits_; }

private:
  /// Optimizes down to 1 bit per bool
  std::vector<bool> bits_;
  size_t num_hashes_{static_cast<size_t>(10 * std::log(2))};

  size_t hash1(const std::string_view data) const;

  size_t hash2(const std::string_view data) const;

  std::vector<size_t> get_hashes(const std::string_view data) const;
};
} // namespace lsm_storage_engine
