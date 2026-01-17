#pragma once

#include "Constants.h"
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>
namespace lsm_storage_engine {

class Block {
public:
  explicit Block() {}

  /**
   *@brief Append the key and value to the block in disk format.
   *@param key The key to write.
   *@param value The key's value.
   *@return The size in bytes appended to the block's data buffer.
   */
  size_t append(const std::string_view key, const std::string_view value);

  /**
   * @brief Readonly buffer containing the block's data.
   * @return Span containing the block's data.
   */
  std::span<const std::byte> data() const {
    return std::span<const std::byte>{data_};
  }

  [[nodiscard]]
  size_t size() const {
    return data_.size();
  }

  [[nodiscard]]
  bool is_full() const {
    return data_.size() >= lsm_constants::kBlockSize;
  }

  [[nodiscard]]
  const std::optional<std::string_view> first() const {
    return first_key_;
  }

private:
  std::vector<std::byte> data_;
  std::optional<std::string> first_key_;
};

} // namespace lsm_storage_engine
