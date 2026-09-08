#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "common/clock.h"
#include "common/config.h"
#include "gfs.pb.h"
#include "master/chunkserver_registry.h"
#include "master/master_state.h"
#include "master/oplog.h"
#include "master/worker_pool.h"

namespace gfs {

struct GrantResult {
  rpc::ResultCode code = rpc::OK;
  uint64_t version = 0;
  rpc::Replica primary;
  std::vector<rpc::Replica> secondaries;
};

struct RevokeResult {
  bool had_lease = false;
  bool acked = false;
  TimePoint expiry;
};

class LeaseManager {
 public:
  LeaseManager(const Config& config, MasterState& state, ChunkserverRegistry& registry, OpLog& oplog);

  GrantResult grant(uint64_t handle);
  void regrant(uint64_t handle);
  void requestRegrant(uint64_t handle, WorkerPool& pool);
  RevokeResult revoke(uint64_t handle);

  bool isValid(const ChunkMeta& meta, TimePoint now) const;
  bool isPendingExpiry(const ChunkMeta& meta, TimePoint now) const;
  TimePoint waitUntil(const ChunkMeta& meta) const;
  bool extendLocked(ChunkMeta& meta, const std::string& primary, TimePoint now) const;

 private:
  struct Attempt {
    bool possible = false;
    std::string primary;
    std::vector<std::string> secondaries;
    uint64_t version = 0;
    uint64_t seq = 0;
  };

  Attempt prepareLocked(uint64_t handle, TimePoint now);
  GrantResult grantSerialized(uint64_t handle);
  void beginHandle(uint64_t handle);
  void endHandle(uint64_t handle);

  const Config& config_;
  MasterState& state_;
  ChunkserverRegistry& registry_;
  OpLog& oplog_;
  std::mutex busy_mutex_;
  std::condition_variable busy_cv_;
  std::set<uint64_t> busy_;
  std::set<uint64_t> regrant_pending_;
};

}
