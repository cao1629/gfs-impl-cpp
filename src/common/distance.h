#pragma once

#include <string>
#include <vector>

#include "gfs.pb.h"

namespace gfs {

struct Endpoint {
  std::string address;
  std::string rack;
};

int DistanceBetween(const Endpoint& a, const Endpoint& b);

std::vector<rpc::Replica> OrderPushChain(const Endpoint& origin,
                                         std::vector<rpc::Replica> replicas);

}  // namespace gfs
