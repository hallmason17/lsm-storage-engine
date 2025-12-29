#include "BloomFilter.h"
#include "utils/CheckSum.h"
namespace lsm_storage_engine {
void BloomFilter::add(const std::string_view key) {
  for (const auto bit : get_hashes(key)) {
    bits_[bit] = true;
  }
} // namespace lsm_storage_engine
bool BloomFilter::contains(const std::string_view key) {
  for (const auto bit : get_hashes(key)) {
    if (!bits_[bit])
      return false;
  }
  return true;
}

size_t BloomFilter::hash1(const std::string_view data) const {
  return std::hash<std::string>{}(std::string(data));
}

size_t BloomFilter::hash2(const std::string_view data) const {
  return xxhash64(data);
}

std::vector<size_t> BloomFilter::get_hashes(const std::string_view data) const {
  std::vector<size_t> indices(num_hashes_);
  size_t h1 = hash1(data);
  size_t h2 = hash2(data);

  for (size_t i = 0; i < num_hashes_; ++i) {
    indices[i] = (h1 + i * h2) % bits_.size();
  }
  return indices;
}
} // namespace lsm_storage_engine
