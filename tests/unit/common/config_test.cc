#include "common/config.h"

#include <gtest/gtest.h>

namespace gfs {

TEST(Config, ParsesUnits) {
  Millis ms;
  ASSERT_TRUE(ParseDuration("250", &ms));
  EXPECT_EQ(ms.count(), 250);
  ASSERT_TRUE(ParseDuration("1.5s", &ms));
  EXPECT_EQ(ms.count(), 1500);
  ASSERT_TRUE(ParseDuration("3d", &ms));
  EXPECT_EQ(ms.count(), 3LL * 86400 * 1000);
  EXPECT_FALSE(ParseDuration("x", &ms));
  uint64_t size = 0;
  ASSERT_TRUE(ParseSize("1M", &size));
  EXPECT_EQ(size, 1ull << 20);
  ASSERT_TRUE(ParseSize("64K", &size));
  EXPECT_EQ(size, 64ull << 10);
}

TEST(Config, SetsByKey) {
  Config config;
  ASSERT_TRUE(Config::Set(config, "chunk_size", "1M"));
  ASSERT_TRUE(Config::Set(config, "lease_duration", "3s"));
  ASSERT_TRUE(Config::Set(config, "rack", "r1"));
  EXPECT_EQ(config.chunk_size, 1ull << 20);
  EXPECT_EQ(config.lease_duration.count(), 3000);
  EXPECT_EQ(config.rack, "r1");
  EXPECT_FALSE(Config::Set(config, "no_such_key", "1"));
  EXPECT_EQ(config.EffectiveMaxRecordAppendSize(), (1ull << 20) / 4);
  EXPECT_EQ(config.EffectiveOuterRetryDelay(), config.chunkserver_dead_timeout);
}

}  // namespace gfs
