#include <gtest/gtest.h>

#include "chunkserver/data_buffer.h"

namespace gfs {

TEST(DataBuffer, TakeRemovesTheEntry) {
  DataBuffer buffer(1000);
  buffer.put({"c1", 1}, "hello");
  EXPECT_EQ(buffer.entryCount(), 1u);
  EXPECT_EQ(buffer.bytesInUse(), 5u);
  auto data = buffer.take({"c1", 1});
  ASSERT_TRUE(data.has_value());
  EXPECT_EQ(*data, "hello");
  EXPECT_FALSE(buffer.take({"c1", 1}).has_value());
  EXPECT_EQ(buffer.bytesInUse(), 0u);
  EXPECT_FALSE(buffer.take({"c2", 1}).has_value());
}

TEST(DataBuffer, EvictsOldestWhenOverCapacity) {
  DataBuffer buffer(10);
  buffer.put({"c", 1}, "aaaa");
  buffer.put({"c", 2}, "bbbb");
  buffer.put({"c", 3}, "cccc");
  EXPECT_EQ(buffer.entryCount(), 2u);
  EXPECT_FALSE(buffer.take({"c", 1}).has_value());
  EXPECT_EQ(*buffer.take({"c", 2}), "bbbb");
  EXPECT_EQ(*buffer.take({"c", 3}), "cccc");
}

TEST(DataBuffer, ReplacesAnExistingKey) {
  DataBuffer buffer(100);
  buffer.put({"c", 1}, "one");
  buffer.put({"c", 1}, "uno");
  EXPECT_EQ(buffer.entryCount(), 1u);
  EXPECT_EQ(buffer.bytesInUse(), 3u);
  EXPECT_EQ(*buffer.take({"c", 1}), "uno");
}

TEST(DataBuffer, OversizedEntryStillLandsAlone) {
  DataBuffer buffer(4);
  buffer.put({"c", 1}, "ab");
  buffer.put({"c", 2}, "abcdefgh");
  EXPECT_EQ(buffer.entryCount(), 1u);
  EXPECT_EQ(*buffer.take({"c", 2}), "abcdefgh");
}

}
