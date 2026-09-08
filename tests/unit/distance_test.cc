#include <gtest/gtest.h>

#include "common/distance.h"

namespace gfs {

namespace {

rpc::Replica replica(const std::string& id, const std::string& address, const std::string& rack) {
  rpc::Replica r;
  r.set_chunkserver_id(id);
  r.set_address(address);
  r.set_rack(rack);
  return r;
}

}

TEST(Distance, RackLabelsWinWhenPresent) {
  EXPECT_EQ(distanceBetween({"10.0.0.1:1", "r1"}, {"10.0.0.2:1", "r1"}), 0);
  EXPECT_EQ(distanceBetween({"10.0.0.1:1", "r1"}, {"10.0.0.2:1", "r2"}), 1);
}

TEST(Distance, IpPrefixWhenNoLabels) {
  EXPECT_EQ(distanceBetween({"10.0.0.1:1", ""}, {"10.0.0.2:1", ""}), 2);
  EXPECT_EQ(distanceBetween({"10.0.0.1:1", ""}, {"10.1.0.1:1", ""}), 16);
  EXPECT_EQ(distanceBetween({"", ""}, {"10.0.0.1:1", ""}), 32);
}

TEST(Distance, ChainVisitsNearestFirst) {
  std::vector<rpc::Replica> replicas = {
      replica("a", "10.1.0.1:1", ""), replica("b", "10.0.0.9:1", ""), replica("c", "10.0.0.2:1", "")};
  auto chain = orderPushChain({"10.0.0.1:0", ""}, replicas);
  ASSERT_EQ(chain.size(), 3u);
  EXPECT_EQ(chain[0].chunkserver_id(), "c");
  EXPECT_EQ(chain[1].chunkserver_id(), "b");
  EXPECT_EQ(chain[2].chunkserver_id(), "a");
}

TEST(Distance, DegeneratesToAChainNotAStar) {
  std::vector<rpc::Replica> replicas = {
      replica("a", "127.0.0.1:1", ""), replica("b", "127.0.0.1:2", ""), replica("c", "127.0.0.1:3", "")};
  auto chain = orderPushChain({"", ""}, replicas);
  ASSERT_EQ(chain.size(), 3u);
}

}
