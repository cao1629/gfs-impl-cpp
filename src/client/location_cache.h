#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/clock.h"
#include "common/config.h"
#include "gfs.pb.h"

namespace gfs {

struct CachedChunk {
  uint64_t handle = 0;
  uint64_t version = 0;
  std::vector<rpc::Replica> replicas;
  TimePoint expiry;
};

struct CachedLease {
  uint64_t handle = 0;
  uint64_t version = 0;
  rpc::Replica primary;
  std::vector<rpc::Replica> secondaries;
  TimePoint expiry;
};

template <typename Entry>
class ExpiringCache {
 public:
  explicit ExpiringCache(Millis ttl) : ttl_(ttl) {}

  std::optional<Entry> get(const std::string& path, uint64_t index) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find({path, index});
    if (it == entries_.end()) return std::nullopt;
    if (it->second.expiry <= now()) {
      entries_.erase(it);
      return std::nullopt;
    }
    return it->second;
  }

  void put(const std::string& path, uint64_t index, Entry entry) {
    entry.expiry = now() + ttl_;
    std::lock_guard<std::mutex> lock(mu_);
    entries_[{path, index}] = std::move(entry);
  }

  void invalidate(const std::string& path, uint64_t index) {
    std::lock_guard<std::mutex> lock(mu_);
    entries_.erase({path, index});
  }

  void invalidateFile(const std::string& path) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.lower_bound({path, 0});
    while (it != entries_.end() && it->first.first == path) it = entries_.erase(it);
  }

 private:
  Millis ttl_;
  std::mutex mu_;
  std::map<std::pair<std::string, uint64_t>, Entry> entries_;
};

using LocationCache = ExpiringCache<CachedChunk>;
using LeaseCache = ExpiringCache<CachedLease>;

}
