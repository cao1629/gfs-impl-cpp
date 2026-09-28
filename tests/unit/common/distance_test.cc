#include "common/distance.h"

#include <gtest/gtest.h>

namespace gfs {

namespace {

rpc::Replica Replica(const std::string& id, const std::string& address,
                     const std::string& rack) {
  rpc::Replica r;
  r.set_chunkserver_id(id);
  r.set_address(address);
  r.set_rack(rack);
  return r;
}

}  // namespace

TEST(Distance, RackLabelsWinWhenPresent) {
  EXPECT_EQ(DistanceBetween({"10.0.0.1:1", "r1"}, {"10.0.0.2:1", "r1"}), 0);
  EXPECT_EQ(DistanceBetween({"10.0.0.1:1", "r1"}, {"10.0.0.2:1", "r2"}), 1);
}

TEST(Distance, IpPrefixWhenNoLabels) {
  EXPECT_EQ(DistanceBetween({"10.0.0.1:1", ""}, {"10.0.0.2:1", ""}), 2);
  EXPECT_EQ(DistanceBetween({"10.0.0.1:1", ""}, {"10.1.0.1:1", ""}), 17);
  EXPECT_EQ(DistanceBetween({"", ""}, {"10.0.0.1:1", ""}), 32);
}

TEST(Distance, ChainVisitsNearestFirst) {
  std::vector<rpc::Replica> replicas = {Replica("a", "10.1.0.1:1", ""),
                                        Replica("b", "10.0.0.9:1", ""),
                                        Replica("c", "10.0.0.2:1", "")};
  auto chain = OrderPushChain({"10.0.0.1:0", ""}, replicas);
  ASSERT_EQ(chain.size(), 3u);
  EXPECT_EQ(chain[0].chunkserver_id(), "c");
  EXPECT_EQ(chain[1].chunkserver_id(), "b");
  EXPECT_EQ(chain[2].chunkserver_id(), "a");
}

TEST(Distance, DegeneratesToAChainNotAStar) {
  std::vector<rpc::Replica> replicas = {Replica("a", "127.0.0.1:1", ""),
                                        Replica("b", "127.0.0.1:2", ""),
                                        Replica("c", "127.0.0.1:3", "")};
  auto chain = OrderPushChain({"", ""}, replicas);
  ASSERT_EQ(chain.size(), 3u);
}

}  // namespace gfs
