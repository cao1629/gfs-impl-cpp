#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

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

  void start();
  void stop();
  void post(std::function<void()> fn);

  void getClusterInfo(const rpc::GetClusterInfoRequest& req, rpc::GetClusterInfoResponse* resp, Done done);
  void create(const rpc::CreateRequest& req, rpc::CreateResponse* resp, Done done);
  void open(const rpc::OpenRequest& req, rpc::OpenResponse* resp, Done done);
  void remove(const rpc::DeleteRequest& req, rpc::DeleteResponse* resp, Done done);
  void rename(const rpc::RenameRequest& req, rpc::RenameResponse* resp, Done done);
  void snapshot(const rpc::SnapshotRequest& req, rpc::SnapshotResponse* resp, Done done);
  void findMatchingFiles(const rpc::FindMatchingFilesRequest& req, rpc::FindMatchingFilesResponse* resp, Done done);
  void findLocation(const rpc::FindLocationRequest& req, rpc::FindLocationResponse* resp, Done done);
  void findLeaseHolder(const rpc::FindLeaseHolderRequest& req, rpc::FindLeaseHolderResponse* resp, Done done);
  void addChunk(const rpc::AddChunkRequest& req, rpc::AddChunkResponse* resp, Done done);
  void heartBeat(const rpc::HeartBeatRequest& req, rpc::HeartBeatResponse* resp, Done done);

  void gcPass();
  void sweepDead();

  MasterState& state() { return state_; }
  LockTable& locks() { return locks_; }
  const Config& config() const { return config_; }

 private:
  struct LeaseCall;
  struct SnapshotCall;

  void recover();
  uint64_t log(const state::LogRecord& record);
  uint64_t allocHandle();
  std::vector<std::string> chooseServers(size_t count);
  std::vector<std::string> createChunkOn(const std::vector<std::string>& ids, uint64_t handle, uint64_t version, uint64_t copy_from);
  std::vector<rpc::Replica> resolveLocationsLocked(const ChunkMeta& meta);
  void fillChunkInfoLocked(rpc::ChunkInfo* info, uint64_t index, uint64_t handle, const ChunkMeta& meta);
  uint64_t copyOnWrite(const std::string& path, uint64_t index, uint64_t old_handle);
  void findLeaseHolderAttempt(std::shared_ptr<LeaseCall> call);
  void snapshotFinish(std::shared_ptr<SnapshotCall> call);
  void revokeLeasesOn(const std::vector<uint64_t>& handles, TimePoint* wait_until);

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

}
