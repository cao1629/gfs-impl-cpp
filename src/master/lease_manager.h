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
  LeaseManager(const Config& config, MasterState& state,
               ChunkserverRegistry& registry, OpLog& oplog);

  GrantResult Grant(uint64_t handle);
  void Regrant(uint64_t handle);
  void RequestRegrant(uint64_t handle, WorkerPool& pool);
  RevokeResult Revoke(uint64_t handle);

  bool IsValid(const ChunkMeta& meta, TimePoint now) const;
  bool IsPendingExpiry(const ChunkMeta& meta, TimePoint now) const;
  TimePoint WaitUntil(const ChunkMeta& meta) const;
  bool ExtendLocked(ChunkMeta& meta, const std::string& primary,
                    TimePoint now) const;

 private:
  struct Attempt {
    bool possible = false;
    std::string primary;
    std::vector<std::string> secondaries;
    uint64_t version = 0;
    uint64_t seq = 0;
  };

  Attempt PrepareLocked(uint64_t handle, TimePoint now);
  GrantResult GrantSerialized(uint64_t handle);
  void BeginHandle(uint64_t handle);
  void EndHandle(uint64_t handle);

  const Config& config_;
  MasterState& state_;
  ChunkserverRegistry& registry_;
  OpLog& oplog_;
  std::mutex busy_mutex_;
  std::condition_variable busy_cv_;
  std::set<uint64_t> busy_;
  std::set<uint64_t> regrant_pending_;
};

}  // namespace gfs
