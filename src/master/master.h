#pragma once

#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "gfs.pb.h"
#include "master/chunkserver_registry.h"
#include "master/lease_manager.h"
#include "master/lock_table.h"
#include "master/master_state.h"
#include "master/oplog.h"
#include "master/timer_queue.h"
#include "master/worker_pool.h"

namespace gfs {

class Master {
 public:
  using Done = std::function<void(grpc::Status)>;

  explicit Master(Config config);
  ~Master();

  void Start();
  void Stop();
  void Post(std::function<void()> fn);

  void GetClusterInfo(const rpc::GetClusterInfoRequest& req,
                      rpc::GetClusterInfoResponse* resp, Done done);
  void Create(const rpc::CreateRequest& req, rpc::CreateResponse* resp,
              Done done);
  void Open(const rpc::OpenRequest& req, rpc::OpenResponse* resp, Done done);
  void Remove(const rpc::DeleteRequest& req, rpc::DeleteResponse* resp,
              Done done);
  void Rename(const rpc::RenameRequest& req, rpc::RenameResponse* resp,
              Done done);
  void Snapshot(const rpc::SnapshotRequest& req, rpc::SnapshotResponse* resp,
                Done done);
  void FindMatchingFiles(const rpc::FindMatchingFilesRequest& req,
                         rpc::FindMatchingFilesResponse* resp, Done done);
  void FindLocation(const rpc::FindLocationRequest& req,
                    rpc::FindLocationResponse* resp, Done done);
  void FindLeaseHolder(const rpc::FindLeaseHolderRequest& req,
                       rpc::FindLeaseHolderResponse* resp, Done done);
  void AddChunk(const rpc::AddChunkRequest& req, rpc::AddChunkResponse* resp,
                Done done);
  void HeartBeat(const rpc::HeartBeatRequest& req, rpc::HeartBeatResponse* resp,
                 Done done);

  void GcPass();
  void SweepDead();

  MasterState& State() { return state_; }
  LockTable& Locks() { return locks_; }
  // NOLINTNEXTLINE(readability-identifier-naming)
  const Config& config() const { return config_; }

 private:
  struct LeaseCall;
  struct SnapshotCall;

  void Recover();
  uint64_t Log(const state::LogRecord& record);
  uint64_t AllocHandle();
  std::vector<std::string> ChooseServers(size_t count);
  std::vector<std::string> CreateChunkOn(const std::vector<std::string>& ids,
                                         uint64_t handle, uint64_t version,
                                         uint64_t copy_from);
  std::vector<rpc::Replica> ResolveLocationsLocked(const ChunkMeta& meta);
  void FillChunkInfoLocked(rpc::ChunkInfo* info, uint64_t index,
                           uint64_t handle, const ChunkMeta& meta);
  uint64_t CopyOnWrite(const std::string& path, uint64_t index,
                       uint64_t old_handle);
  void FindLeaseHolderAttempt(std::shared_ptr<LeaseCall> call);
  void SnapshotFinish(std::shared_ptr<SnapshotCall> call);
  void RevokeLeasesOn(const std::vector<uint64_t>& handles,
                      TimePoint* wait_until);

  Config config_;
  MasterState state_;
  LockTable locks_;
  WorkerPool pool_;
  TimerQueue timers_;
  OpLog oplog_;
  ChunkserverRegistry registry_;
  LeaseManager leases_;
  std::mutex cow_mutex_;
  std::unique_ptr<PeriodicTask> sweep_;
  std::unique_ptr<PeriodicTask> gc_;
  bool started_ = false;
};

}  // namespace gfs
