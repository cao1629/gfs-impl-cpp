#pragma once

#include <string>
#include <vector>

#include "gfs.pb.h"

namespace gfs {

struct Endpoint {
  std::string address;
  std::string rack;
};

int distanceBetween(const Endpoint& a, const Endpoint& b);

std::vector<rpc::Replica> orderPushChain(const Endpoint& origin, std::vector<rpc::Replica> replicas);

}
