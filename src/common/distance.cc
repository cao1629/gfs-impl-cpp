#include "common/distance.h"

#include <arpa/inet.h>

#include <algorithm>
#include <cstring>

namespace gfs {

namespace {

bool ParseIpv4(const std::string& address, uint32_t* out) {
  size_t colon = address.rfind(':');
  std::string host =
      colon == std::string::npos ? address : address.substr(0, colon);
  in_addr addr{};
  if (inet_pton(AF_INET, host.c_str(), &addr) != 1) return false;
  *out = ntohl(addr.s_addr);
  return true;
}

int CommonPrefixBits(uint32_t a, uint32_t b) {
  uint32_t x = a ^ b;
  int bits = 0;
  while (bits < 32 && (x & (0x80000000u >> bits)) == 0) ++bits;
  return bits;
}

}  // namespace

int DistanceBetween(const Endpoint& a, const Endpoint& b) {
  if (!a.rack.empty() && !b.rack.empty()) return a.rack == b.rack ? 0 : 1;
  uint32_t ia = 0, ib = 0;
  if (!ParseIpv4(a.address, &ia) || !ParseIpv4(b.address, &ib)) return 32;
  return 32 - CommonPrefixBits(ia, ib);
}

std::vector<rpc::Replica> OrderPushChain(const Endpoint& origin,
                                         std::vector<rpc::Replica> replicas) {
  std::vector<rpc::Replica> chain;
  Endpoint current = origin;
  while (!replicas.empty()) {
    auto best = std::min_element(
        replicas.begin(), replicas.end(),
        [&](const rpc::Replica& x, const rpc::Replica& y) {
          return DistanceBetween(current, {x.address(), x.rack()}) <
                 DistanceBetween(current, {y.address(), y.rack()});
        });
    current = {best->address(), best->rack()};
    chain.push_back(*best);
    replicas.erase(best);
  }
  return chain;
}

}  // namespace gfs
