#include "common/framing.h"

#include <gtest/gtest.h>

namespace gfs {

TEST(Framing, RoundTripsSeveralRecords) {
  std::string bytes = EncodeRecord("alpha") + EncodeRecord("") +
                      EncodeRecord(std::string(70000, 'x'));
  DecodedRecords decoded = DecodeRecords(bytes);
  ASSERT_EQ(decoded.payloads.size(), 3u);
  EXPECT_EQ(decoded.payloads[0], "alpha");
  EXPECT_EQ(decoded.payloads[1], "");
  EXPECT_EQ(decoded.payloads[2].size(), 70000u);
  EXPECT_FALSE(decoded.torn_tail);
  EXPECT_EQ(decoded.consumed, bytes.size());
}

TEST(Framing, StopsAtTornTail) {
  std::string bytes = EncodeRecord("first") + EncodeRecord("second");
  bytes.resize(bytes.size() - 2);
  DecodedRecords decoded = DecodeRecords(bytes);
  ASSERT_EQ(decoded.payloads.size(), 1u);
  EXPECT_EQ(decoded.payloads[0], "first");
  EXPECT_TRUE(decoded.torn_tail);
  EXPECT_EQ(decoded.consumed, EncodeRecord("first").size());
}

TEST(Framing, RejectsCorruptedPayload) {
  std::string bytes = EncodeRecord("first") + EncodeRecord("second");
  bytes[bytes.size() - 1] ^= 0x01;
  DecodedRecords decoded = DecodeRecords(bytes);
  ASSERT_EQ(decoded.payloads.size(), 1u);
  EXPECT_TRUE(decoded.torn_tail);
}

}  // namespace gfs
