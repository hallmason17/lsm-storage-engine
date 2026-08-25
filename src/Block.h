#pragma once

#include "Constants.h"
#include "StorageError.h"
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
namespace lsm {

class Block {
public:
  explicit Block() {}

  /**
   * @brief Serialized size of a key-value record in disk format.
   */
  static size_t encoded_size(std::string_view key, std::string_view value);

  /**
   * @brief Decode one record at offset and advance offset past it.
   * @return The key-value pair, nullopt at end of data, or StorageError
   *         on truncation/checksum failure.
   */
  static std::expected<std::optional<std::pair<std::string, std::string>>,
                       StorageError>
  decode_entry(std::span<const std::byte> data, size_t &offset);

  /**
   * @brief Search a block's bytes for a key.
   * @return The value if found, nullopt if not, or StorageError on corruption.
   */
  static std::expected<std::optional<std::string>, StorageError>
  find(std::span<const std::byte> data, std::string_view key);

  /**
   *@brief Append the key and value to the block in disk format.
   *@param key The key to write.
   *@param value The key's value.
   *@return The size in bytes appended to the block's data buffer, or 0 if
   *        the record does not fit (unless the block is empty).
   */
  size_t append(std::string_view key, std::string_view value);

  /**
   * @brief Search this block for a key.
   */
  std::expected<std::optional<std::string>, StorageError>
  find(std::string_view key) const {
    return find(data(), key);
  }

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
    return data_.size() >= constants::kBlockSize;
  }

  [[nodiscard]]
  const std::optional<std::string_view> first() const {
    return first_key_;
  }

private:
  std::vector<std::byte> data_;
  std::optional<std::string> first_key_;
};

} // namespace lsm
