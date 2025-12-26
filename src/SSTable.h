#pragma once
#include "Constants.h"
#include "StorageError.h"
#include <expected>
#include <filesystem>
#include <optional>
#include <span>
namespace lsm_storage_engine {

/**
 * @brief Immutable on-disk sorted string table.
 *
 * SSTable (Sorted String Table) stores key-value pairs in sorted order on
 * disk. Each SSTable is created by flushing a MemTable and is immutable once
 * written. Keys are stored in lexicographic order to support efficient lookups
 * and range scans.
 *
 * Read operations are thread-safe. The table is immutable after creation.
 */
class SSTable {
public:
  SSTable() {}

  /**
   * @brief Creates a new SSTable with a generated filename.
   *
   * The filename is the current timestamp to ensure uniqueness.
   * @return SSTable on success, StorageError if the file cannot be created.
   */
  static std::expected<SSTable, StorageError> create();

  /**
   * @brief Creates a new SSTable at the specified path.
   * @param path Path for the new SSTable file.
   * @return SSTable on success, StorageError if the file cannot be created.
   */
  static std::expected<SSTable, StorageError> create(std::filesystem::path path);

  /**
   * @brief Opens an existing SSTable from the specified path.
   * @param path Path to the SSTable file.
   * @return SSTable on success, StorageError if the file cannot be opened.
   */
  static std::expected<SSTable, StorageError> open(const std::filesystem::path);

  /**
   * @brief Constructs an SSTable with the given path (does not open file).
   * @param path Path to the SSTable file.
   */
  SSTable(std::filesystem::path path) : path_(std::move(path)) {}

  ~SSTable() { close_file(); }

  // No copies.
  SSTable(const SSTable &) = delete;
  SSTable &operator=(const SSTable &) = delete;

  // Moves OK.
  SSTable(SSTable &&other) noexcept;
  SSTable &operator=(SSTable &&other) noexcept;

  /**
   * @brief Returns the file path of this SSTable.
   */
  const std::filesystem::path &path() const { return path_; }

  /**
   * @brief Searches for a key in the SSTable.
   * @param key The key to look up.
   * @return The value if found, std::nullopt if not found, or StorageError
   *         on I/O failure.
   */
  std::expected<std::optional<std::string>, StorageError>
  get(std::string_view key);

  std::expected<std::optional<std::pair<std::string, std::string>>,
                StorageError>
  next();

  std::expected<std::optional<std::pair<std::string, std::string>>,
                StorageError>
  read_entry() const;
  std::expected<void, StorageError> write_entry(const std::string_view key,
                                                const std::string_view value);

  bool marked_for_delete_{false};

  struct Header {
    std::string min_key;
    std::string max_key;
    /**
     * Serialized size in bytes.
     */
    size_t size;
    Header() : min_key(), max_key(), size(0) {}
    Header(std::string min, std::string max)
        : min_key(std::move(min)), max_key(std::move(max)),
          size(this->min_key.size() + sizeof(uint32_t) + this->max_key.size() +
               sizeof(uint32_t)) {

      // Serialized size in bytes:
      // [num_entries:4][min_key_len:4][min_key][max_key_len:4][max_key]
    }
  };
  struct Footer {
    size_t index_offset;
    size_t index_size;
    size_t num_index_entries;
    size_t magic_num;
    Footer()
        : index_offset(0), index_size(0), num_index_entries(0),
          magic_num(lsm_constants::kMagicNumber) {}
    Footer(size_t offset, size_t size, size_t num_entries)
        : index_offset(offset), index_size(size),
          num_index_entries(num_entries),
          magic_num(lsm_constants::kMagicNumber) {}
  };
  std::expected<Header, StorageError> read_header();
  std::expected<Footer, StorageError> read_footer();
  std::expected<void, StorageError> write_header(Header &&header);
  std::expected<void, StorageError> write_footer(Footer footer);
  [[nodiscard]]
  const Header &header() const {
    return header_;
  }
  [[nodiscard]]
  const Footer &footer() const {
    return footer_;
  }

private:
  std::filesystem::path path_;
  int fd_{-1};
  off_t file_pos_{0};
  std::span<std::byte> mapped_data_;
  size_t file_size_{0};
  Header header_;
  Footer footer_;
  // TODO: Add a refcount

  /**
   * @brief Opens the SSTable file for reading.
   * @return void on success, StorageError on failure.
   */
  std::expected<void, StorageError> open_file();

  std::expected<void, StorageError> ensure_mapped();

  /**
   * @brief Closes the file descriptor if open.
   */
  void close_file();
};
} // namespace lsm_storage_engine
