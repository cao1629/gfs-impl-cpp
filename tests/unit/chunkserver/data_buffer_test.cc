#include "chunkserver/data_buffer.h"

#include <gtest/gtest.h>

namespace gfs {

TEST(DataBuffer, TakeRemovesTheEntry) {
  DataBuffer buffer(1000);
  buffer.Put({"c1", 1}, "hello");
  EXPECT_EQ(buffer.EntryCount(), 1u);
  EXPECT_EQ(buffer.BytesInUse(), 5u);
  auto data = buffer.Take({"c1", 1});
  ASSERT_TRUE(data.has_value());
  EXPECT_EQ(*data, "hello");
  EXPECT_FALSE(buffer.Take({"c1", 1}).has_value());
  EXPECT_EQ(buffer.BytesInUse(), 0u);
  EXPECT_FALSE(buffer.Take({"c2", 1}).has_value());
}

TEST(DataBuffer, EvictsOldestWhenOverCapacity) {
  DataBuffer buffer(10);
  buffer.Put({"c", 1}, "aaaa");
  buffer.Put({"c", 2}, "bbbb");
  buffer.Put({"c", 3}, "cccc");
  EXPECT_EQ(buffer.EntryCount(), 2u);
  EXPECT_FALSE(buffer.Take({"c", 1}).has_value());
  EXPECT_EQ(*buffer.Take({"c", 2}), "bbbb");
  EXPECT_EQ(*buffer.Take({"c", 3}), "cccc");
}

TEST(DataBuffer, ReplacesAnExistingKey) {
  DataBuffer buffer(100);
  buffer.Put({"c", 1}, "one");
  buffer.Put({"c", 1}, "uno");
  EXPECT_EQ(buffer.EntryCount(), 1u);
  EXPECT_EQ(buffer.BytesInUse(), 3u);
  EXPECT_EQ(*buffer.Take({"c", 1}), "uno");
}

TEST(DataBuffer, OversizedEntryStillLandsAlone) {
  DataBuffer buffer(4);
  buffer.Put({"c", 1}, "ab");
  buffer.Put({"c", 2}, "abcdefgh");
  EXPECT_EQ(buffer.EntryCount(), 1u);
  EXPECT_EQ(*buffer.Take({"c", 2}), "abcdefgh");
}

}  // namespace gfs
