#include "Block.h"
#include "Constants.h"
#include <gtest/gtest.h>
#include <string>

using namespace lsm;

TEST(BlockTest, AppendSetsFirstKey) {
  Block block;
  EXPECT_FALSE(block.first().has_value());
  EXPECT_EQ(block.append("alpha", "1"), Block::encoded_size("alpha", "1"));
  ASSERT_TRUE(block.first().has_value());
  EXPECT_EQ(*block.first(), "alpha");
}

TEST(BlockTest, AppendPacksDistinctKeys) {
  Block block;
  EXPECT_GT(block.append("a", "1"), 0U);
  EXPECT_GT(block.append("b", "2"), 0U);
  EXPECT_GT(block.append("c", "3"), 0U);

  EXPECT_EQ(*block.find("a"), "1");
  EXPECT_EQ(*block.find("b"), "2");
  EXPECT_EQ(*block.find("c"), "3");
  EXPECT_FALSE(block.find("d")->has_value());
}

TEST(BlockTest, AppendRejectsWhenFullKeepsFirstKey) {
  Block block;
  ASSERT_GT(block.append("first", "v"), 0U);

  std::string big_key(100, 'k');
  std::string big_val(constants::kBlockSize, 'v');
  EXPECT_EQ(block.append(big_key, big_val), 0U);
  EXPECT_EQ(*block.first(), "first");
  EXPECT_EQ(*block.find("first"), "v");
}

TEST(BlockTest, EmptyBlockAcceptsOversizedEntry) {
  Block block;
  std::string key(1000, 'k');
  std::string value(5000, 'v');
  EXPECT_EQ(block.append(key, value), Block::encoded_size(key, value));
  EXPECT_GT(block.size(), constants::kBlockSize);
  EXPECT_EQ(*block.find(key), value);
}

TEST(BlockTest, DecodeRoundTrip) {
  Block block;
  block.append("key", "value");
  size_t offset = 0;
  auto entry = Block::decode_entry(block.data(), offset);
  ASSERT_TRUE(entry.has_value());
  ASSERT_TRUE(entry->has_value());
  EXPECT_EQ(entry->value().first, "key");
  EXPECT_EQ(entry->value().second, "value");
  EXPECT_EQ(offset, block.size());
}
