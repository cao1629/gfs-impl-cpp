#include "chunkserver/lease_table.h"

#include <gtest/gtest.h>

#include <vector>

namespace gfs {

TEST(LeaseTable, AsksForExtensionOnlyAfterAMutation) {
  LeaseTable leases(Millis(0));
  leases.Grant(7, Millis(60'000), {});
  EXPECT_TRUE(leases.HandlesToExtend().empty());

  leases.NextSerial(7);
  EXPECT_EQ(leases.HandlesToExtend(), std::vector<uint64_t>{7});
  EXPECT_EQ(leases.HandlesToExtend(), std::vector<uint64_t>{7});

  ASSERT_TRUE(leases.Extend(7, Millis(60'000)));
  EXPECT_TRUE(leases.HandlesToExtend().empty());

  leases.NextSerial(7);
  EXPECT_EQ(leases.HandlesToExtend(), std::vector<uint64_t>{7});
  leases.Grant(7, Millis(60'000), {});
  EXPECT_TRUE(leases.HandlesToExtend().empty());
}

TEST(LeaseTable, NeverAsksForAnExpiredOrRevokedLease) {
  LeaseTable leases(Millis(1));
  leases.Grant(1, Millis(0), {});
  leases.NextSerial(1);
  leases.Grant(2, Millis(60'000), {});
  leases.NextSerial(2);
  leases.Revoke(2);
  EXPECT_TRUE(leases.HandlesToExtend().empty());
}

}  // namespace gfs
