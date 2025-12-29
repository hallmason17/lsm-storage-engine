#include "SSTable.h"
#include "Constants.h"
#include "StorageError.h"
#include "utils/CheckSum.h"
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <expected>
#include <fcntl.h>
#include <filesystem>
#include <optional>
#include <print>
#include <span>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>
namespace lsm_storage_engine {

SSTable::SSTable(SSTable &&other) noexcept
    : path_{std::move(other.path_)}, fd_{std::exchange(other.fd_, -1)},
      file_pos_{std::exchange(other.file_pos_, 0)},
      mapped_data_{std::exchange(other.mapped_data_, {})},
      file_size_{std::exchange(other.file_size_, 0)},
      header_{std::move(other.header_)}, footer_{other.footer_},
      index_{std::move(other.index_)},
      bloom_filter_{std::move(other.bloom_filter_)} {}

SSTable &SSTable::operator=(SSTable &&other) noexcept {
  if (this != &other) {
    close_file();
    path_ = std::move(other.path_);
    fd_ = std::exchange(other.fd_, -1);
    mapped_data_ = std::exchange(other.mapped_data_, {});
    file_pos_ = std::exchange(other.file_pos_, 0);
    file_size_ = std::exchange(other.file_size_, 0);
    header_ = std::move(other.header_);
    footer_ = other.footer_;
    index_ = std::move(other.index_);
    bloom_filter_ = std::move(other.bloom_filter_);
  }
  return *this;
}

std::expected<void, StorageError> SSTable::open_file() {
  fd_ = ::open(path_.c_str(), O_CREAT | O_RDWR, 0644);
  if (fd_ == -1) {
    return std::unexpected(StorageError::file_open(path()));
  }
  return {};
}

void SSTable::close_file() {
  if (fd_ != -1) {
    ::close(fd_);
    fd_ = -1;
  }
  if (mapped_data_.data() != nullptr) {
    ::munmap(mapped_data_.data(), file_size_);
  }
}

std::expected<std::optional<std::string>, StorageError>
SSTable::get(std::string_view key) {
  if (key < header().min_key || key > header().max_key) {
    return std::nullopt;
  }
  if (bloom_filter_.bits().size() > 0 && !bloom_filter_.contains(key)) {
    return std::nullopt;
  }
  // Calculate the correct position after header and bloom filter
  size_t bloom_filter_size =
      sizeof(size_t) + bloom_filter_.bits().size() * sizeof(bool);
  size_t jump_to{header().size + bloom_filter_size};
  auto it = std::ranges::upper_bound(index_, key, std::ranges::less{},
                                     &IndexEntry::key);
  if (it != index_.begin()) {
    --it;
    jump_to = it->file_position;
  }

  file_pos_ = static_cast<off_t>(jump_to);
  for (size_t i = 0; i < lsm_constants::kIndexSpace; ++i) {
    auto entry = next();
    if (!entry)
      return std::unexpected{entry.error()};
    if (!entry->has_value())
      return std::nullopt;

    if (entry->value().first == key) {
      return entry->value().second;
    }
  }
  return std::nullopt;
}
std::expected<SSTable, StorageError> SSTable::create() {
  SSTable sst;
  auto now = std::chrono::steady_clock::now().time_since_epoch();
  sst.path_ = std::to_string(now.count()) + ".sst";
  if (auto res = sst.open_file(); !res) {
    return std::unexpected{res.error()};
  }
  return sst;
}
std::expected<SSTable, StorageError>
SSTable::create(std::filesystem::path path) {
  SSTable sst;
  sst.path_ = std::move(path);
  if (auto res = sst.open_file(); !res) {
    return std::unexpected{res.error()};
  }
  return sst;
}
std::expected<SSTable, StorageError>
SSTable::open(const std::filesystem::path &path) {
  SSTable sst{path};
  if (auto res = sst.open_file(); !res) {
    return std::unexpected{res.error()};
  }
  if (auto res = sst.ensure_mapped(); !res) {
    return std::unexpected{res.error()};
  }
  if (auto res = sst.read_header(); !res) {
    return std::unexpected{res.error()};
  }
  if (auto res = sst.read_bloom_filter(); !res) {
    return std::unexpected{res.error()};
  }
  if (auto res = sst.read_footer(); !res) {
    return std::unexpected{res.error()};
  }
  if (auto res = sst.read_index(); !res) {
    return std::unexpected{res.error()};
  }
  return sst;
}

std::expected<std::optional<std::pair<std::string, std::string>>, StorageError>
SSTable::read_entry() const {
  if (file_size_ == 0) {
    return std::nullopt;
  }
  if (mapped_data_.data() == nullptr) {
    return std::unexpected(StorageError::file_open(path()));
  }
  // Stop before the index
  size_t data_end = footer().index_offset;
  if (static_cast<size_t>(file_pos_) >= data_end) {
    return std::nullopt;
  }
  uint32_t keylen{0};
  uint32_t valuelen{0};
  uint32_t file_checksum{0};
  ::memcpy(&keylen, mapped_data_.data() + file_pos_, sizeof(keylen));
  ::memcpy(&valuelen, mapped_data_.data() + file_pos_ + sizeof(uint32_t),
           sizeof(valuelen));

  size_t entry_size =
      2 * sizeof(uint32_t) + keylen + valuelen + sizeof(uint32_t);
  if (static_cast<size_t>(file_pos_) + entry_size > data_end) {
    return std::unexpected(StorageError{
        .kind = StorageError::Kind::FileRead,
        .message = "Corrupted SSTable: entry extends into footer",
        .path = path(),
    });
  }

  std::string k(keylen, '\0');
  std::string val(valuelen, '\0');
  ::memcpy(k.data(), mapped_data_.data() + file_pos_ + 2 * sizeof(uint32_t),
           keylen);
  ::memcpy(val.data(),
           mapped_data_.data() + file_pos_ + 2 * sizeof(uint32_t) + k.size(),
           valuelen);
  ::memcpy(&file_checksum,
           mapped_data_.data() + file_pos_ + 2 * sizeof(uint32_t) + k.size() +
               val.size(),
           sizeof(file_checksum));

  auto datalen =
      static_cast<uint32_t>(2 * sizeof(uint32_t) + keylen + valuelen);
  auto checksum =
      hash32({reinterpret_cast<const char *>(mapped_data_.data() + file_pos_),
              datalen});

  if (file_checksum != checksum) {
    return std::unexpected(StorageError{
        .kind = StorageError::Kind::FileRead,
        .message = "Checksum mismatch",
        .path = path(),
    });
  }
  return {{{std::move(k), std::move(val)}}};
}

std::expected<std::optional<std::pair<std::string, std::string>>, StorageError>
SSTable::next() {
  size_t bloom_filter_size =
      sizeof(size_t) + bloom_filter_.bits().size() * sizeof(bool);
  size_t data_start = header().size + bloom_filter_size;
  if (file_pos_ < static_cast<off_t>(data_start)) {
    file_pos_ = static_cast<off_t>(data_start);
  }
  auto entry = ensure_mapped().and_then([&] { return read_entry(); });

  if (!entry) {
    return std::unexpected(entry.error());
  }
  if (!entry->has_value())
    return std::nullopt;
  auto &[k, v] = entry->value();

  // [keysize][valuesize][key][val][checksum]
  file_pos_ += static_cast<off_t>(sizeof(uint32_t) * 2 + k.size() + v.size() +
                                  sizeof(uint32_t));
  return {{{std::move(k), std::move(v)}}};
}
std::expected<size_t, StorageError>
SSTable::write_entry(const std::string_view key,
                     const std::string_view value) const {
  std::vector<std::byte> write_buffer;
  auto keylen = static_cast<uint32_t>(key.size());
  auto valuelen = static_cast<uint32_t>(value.size());

  auto append = [&write_buffer](const void *d, size_t len) {
    auto data = reinterpret_cast<const std::byte *>(d);
    write_buffer.insert(write_buffer.end(), data, data + len);
  };

  append(&keylen, sizeof(keylen));
  append(&valuelen, sizeof(valuelen));
  append(key.data(), key.size());
  append(value.data(), value.size());

  auto cs = hash32({reinterpret_cast<const char *>(write_buffer.data()),
                    write_buffer.size()});

  append(&cs, sizeof(cs));

  if (::write(fd_, write_buffer.data(), write_buffer.size()) !=
      static_cast<ssize_t>(write_buffer.size())) {
    return std::unexpected(StorageError::file_write(path()));
  }

  return write_buffer.size();
}
std::expected<void, StorageError> SSTable::ensure_mapped() {
  if (mapped_data_.data() == nullptr) {
    file_size_ = std::filesystem::file_size(path());
    if (file_size_ > 0) {
      void *addr = ::mmap(nullptr, file_size_, PROT_READ, MAP_PRIVATE, fd_, 0);
      if (addr == MAP_FAILED) {
        return std::unexpected(StorageError::file_read(path_.string()));
      }
      mapped_data_ =
          std::span<std::byte>{static_cast<std::byte *>(addr), file_size_};
    }
  }
  return {};
}
std::expected<void, StorageError> SSTable::write_header(Header &&header) {
  header_ = header;
  std::vector<std::byte> write_buffer;
  auto min_len = static_cast<uint32_t>(header_.min_key.size());
  auto max_len = static_cast<uint32_t>(header_.max_key.size());

  auto append = [&write_buffer](const void *d, size_t len) {
    auto data = reinterpret_cast<const std::byte *>(d);
    write_buffer.insert(write_buffer.end(), data, data + len);
  };

  append(&min_len, sizeof(min_len));
  append(header_.min_key.data(), header_.min_key.size());
  append(&max_len, sizeof(max_len));
  append(header_.max_key.data(), header_.max_key.size());

  if (::write(fd_, write_buffer.data(), write_buffer.size()) !=
      static_cast<ssize_t>(write_buffer.size())) {
    return std::unexpected(StorageError::file_write(path()));
  }

  return {};
}
std::expected<SSTable::Header, StorageError> SSTable::read_header() {
  file_pos_ = 0;
  auto header =
      ensure_mapped().and_then([&] -> std::expected<Header, StorageError> {
        uint32_t min_key_len{0};
        uint32_t max_key_len{0};

        ::memcpy(&min_key_len, mapped_data_.data() + file_pos_,
                 sizeof(min_key_len));

        std::string min_key(min_key_len, '\0');
        ::memcpy(min_key.data(),
                 mapped_data_.data() + file_pos_ + sizeof(uint32_t),
                 min_key_len);

        ::memcpy(&max_key_len,
                 mapped_data_.data() + file_pos_ + sizeof(uint32_t) +
                     min_key_len,
                 sizeof(max_key_len));
        std::string max_key(max_key_len, '\0');

        ::memcpy(max_key.data(),
                 mapped_data_.data() + file_pos_ + 2 * sizeof(uint32_t) +
                     min_key_len,
                 max_key_len);

        return Header{min_key, max_key};
      });
  if (!header) {
    return std::unexpected{header.error()};
  }
  header_ = header.value();
  return header_;
}

std::expected<void, StorageError> SSTable::write_footer(Footer footer) {
  footer_ = footer;
  std::vector<std::byte> write_buffer;

  auto append = [&write_buffer](const void *d, size_t len) {
    auto data = reinterpret_cast<const std::byte *>(d);
    write_buffer.insert(write_buffer.end(), data, data + len);
  };

  // Format: [index_offset:8][index_size:8][num_index_entries:8][magic_num:8]
  append(&footer.index_offset, sizeof(footer.index_offset));
  append(&footer.index_size, sizeof(footer.index_size));
  append(&footer.num_index_entries, sizeof(footer.num_index_entries));
  append(&footer.magic_num, sizeof(footer.magic_num));

  if (::write(fd_, write_buffer.data(), write_buffer.size()) !=
      static_cast<ssize_t>(write_buffer.size())) {
    return std::unexpected(StorageError::file_write(path()));
  }

  return {};
}
std::expected<SSTable::Footer, StorageError> SSTable::read_footer() {
  auto footer =
      ensure_mapped().and_then([&] -> std::expected<Footer, StorageError> {
        if (file_size_ < sizeof(Footer)) {
          return std::unexpected{StorageError::file_read(path())};
        }

        size_t offset = file_size_ - sizeof(Footer);

        // Format:
        // [index_offset:8][index_size:8][num_index_entries:8][magic_num:8]
        size_t index_offset{0};
        size_t index_size{0};
        size_t num_index_entries{0};
        size_t magic_num{0};

        ::memcpy(&index_offset, mapped_data_.data() + offset,
                 sizeof(index_offset));
        ::memcpy(&index_size, mapped_data_.data() + offset + sizeof(size_t),
                 sizeof(index_size));
        ::memcpy(&num_index_entries,
                 mapped_data_.data() + offset + 2 * sizeof(size_t),
                 sizeof(num_index_entries));
        ::memcpy(&magic_num, mapped_data_.data() + offset + 3 * sizeof(size_t),
                 sizeof(magic_num));

        if (magic_num != lsm_constants::kMagicNumber) {
          return std::unexpected{StorageError{
              .kind = StorageError::Kind::FileRead,
              .message = "Invalid magic number in footer",
              .path = path(),
          }};
        }

        return Footer{index_offset, index_size, num_index_entries};
      });

  if (!footer) {
    return std::unexpected{footer.error()};
  }
  footer_ = footer.value();
  return footer_;
}
std::expected<size_t, StorageError> SSTable::write_index() {
  std::vector<std::byte> write_buffer;

  auto append = [&write_buffer](const void *d, size_t len) {
    auto data = reinterpret_cast<const std::byte *>(d);
    write_buffer.insert(write_buffer.end(), data, data + len);
  };

  for (const auto &[key, fpos] : index()) {
    auto keylen = static_cast<uint32_t>(key.size());
    // Format: [key_len:8][key:key_len][fpos:8]
    append(&keylen, sizeof(keylen));
    append(key.data(), key.size());
    append(&fpos, sizeof(fpos));
  }

  if (::write(fd_, write_buffer.data(), write_buffer.size()) !=
      static_cast<ssize_t>(write_buffer.size())) {
    return std::unexpected(StorageError::file_write(path()));
  }

  return write_buffer.size();
}
std::expected<void, StorageError> SSTable::read_index() {
  if (!index_.empty()) {
    return {};
  }
  auto idx = ensure_mapped().and_then([&] -> std::expected<void, StorageError> {
    if (file_size_ < footer_.index_size) {
      return std::unexpected{StorageError::file_read(path())};
    }

    size_t read_pos = footer_.index_offset;

    while (index_.size() < footer_.num_index_entries) {
      uint32_t key_len{0};
      size_t fpos{0};

      ::memcpy(&key_len, mapped_data_.data() + read_pos, sizeof(key_len));

      std::string key(key_len, '\0');
      ::memcpy(key.data(), mapped_data_.data() + read_pos + sizeof(uint32_t),
               key_len);

      ::memcpy(&fpos,
               mapped_data_.data() + read_pos + sizeof(uint32_t) + key_len,
               sizeof(fpos));
      index_.emplace_back(key, fpos);

      read_pos += sizeof(uint32_t) + key_len + sizeof(fpos);
    }

    return {};
  });

  if (!idx) {
    return std::unexpected{idx.error()};
  }
  return {};
}

[[nodiscard]]
std::expected<size_t, StorageError>
SSTable::write_bloom_filter(BloomFilter &&bf) {
  std::vector<std::byte> write_buffer;

  auto append = [&write_buffer](const void *d, size_t len) {
    auto data = reinterpret_cast<const std::byte *>(d);
    write_buffer.insert(write_buffer.end(), data, data + len);
  };

  size_t bf_size = bf.bits().size();
  append(&bf_size, sizeof(size_t));
  if (bf_size > 0) {
    for (bool bit : bf.bits()) {
      append(&bit, sizeof(bit));
    }
  }
  if (::write(fd_, write_buffer.data(), write_buffer.size()) !=
      static_cast<ssize_t>(write_buffer.size())) {
    return std::unexpected(StorageError::file_write(path()));
  }

  bloom_filter_ = std::move(bf);
  return write_buffer.size();
}

std::expected<BloomFilter, StorageError> SSTable::read_bloom_filter() {
  // After header
  file_pos_ = static_cast<off_t>(header_.size);
  size_t bf_size{0};
  ::memcpy(&bf_size, mapped_data_.data() + file_pos_, sizeof(bf_size));

  if (bf_size == 0) {
    bloom_filter_ = BloomFilter{};
    return bloom_filter_;
  }

  std::vector<bool> bf_bits(bf_size);

  for (size_t i = 0; i < bf_bits.size(); ++i) {
    bool byte{0};
    ::memcpy(&byte,
             mapped_data_.data() + file_pos_ + sizeof(bf_size) +
                 i * sizeof(byte),
             sizeof(byte));
    bf_bits[i] = (byte != 0);
  }

  BloomFilter bf{std::move(bf_bits)};
  bloom_filter_ = std::move(bf);

  return bloom_filter_;
}
} // namespace lsm_storage_engine
