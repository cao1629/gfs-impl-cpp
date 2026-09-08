#include <gtest/gtest.h>

#include "common/framing.h"

namespace gfs {

TEST(Framing, RoundTripsSeveralRecords) {
  std::string bytes = encodeRecord("alpha") + encodeRecord("") + encodeRecord(std::string(70000, 'x'));
  DecodedRecords decoded = decodeRecords(bytes);
  ASSERT_EQ(decoded.payloads.size(), 3u);
  EXPECT_EQ(decoded.payloads[0], "alpha");
  EXPECT_EQ(decoded.payloads[1], "");
  EXPECT_EQ(decoded.payloads[2].size(), 70000u);
  EXPECT_FALSE(decoded.torn_tail);
  EXPECT_EQ(decoded.consumed, bytes.size());
}

TEST(Framing, StopsAtTornTail) {
  std::string bytes = encodeRecord("first") + encodeRecord("second");
  bytes.resize(bytes.size() - 2);
  DecodedRecords decoded = decodeRecords(bytes);
  ASSERT_EQ(decoded.payloads.size(), 1u);
  EXPECT_EQ(decoded.payloads[0], "first");
  EXPECT_TRUE(decoded.torn_tail);
  EXPECT_EQ(decoded.consumed, encodeRecord("first").size());
}

TEST(Framing, RejectsCorruptedPayload) {
  std::string bytes = encodeRecord("first") + encodeRecord("second");
  bytes[bytes.size() - 1] ^= 0x01;
  DecodedRecords decoded = decodeRecords(bytes);
  ASSERT_EQ(decoded.payloads.size(), 1u);
  EXPECT_TRUE(decoded.torn_tail);
}

}
