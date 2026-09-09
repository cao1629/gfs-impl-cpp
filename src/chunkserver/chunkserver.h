#pragma once

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "chunkserver/chunk_store.h"
#include "chunkserver/data_buffer.h"
#include "chunkserver/lease_table.h"
#include "common/config.h"
#include "gfs.grpc.pb.h"

namespace gfs {

class Chunkserver {
 public:
  Chunkserver(Config config, std::string id, ChunkStore* store);
  ~Chunkserver();

  Chunkserver(const Chunkserver&) = delete;
  Chunkserver& operator=(const Chunkserver&) = delete;

  rpc::PushDataResponse pushData(grpc::ServerReader<rpc::PushDataRequest>* reader);
  rpc::ReadResponse read(const rpc::ReadRequest& req);
  rpc::WriteResponse write(const rpc::WriteRequest& req);
  rpc::RecordAppendResponse recordAppend(const rpc::RecordAppendRequest& req);
  rpc::GetChunkLengthResponse getChunkLength(const rpc::GetChunkLengthRequest& req);
  rpc::ApplyMutationResponse applyMutation(const rpc::ApplyMutationRequest& req);
  rpc::CreateChunkResponse createChunk(const rpc::CreateChunkRequest& req);
  rpc::GrantLeaseResponse grantLease(const rpc::GrantLeaseRequest& req);
  rpc::RevokeLeaseResponse revokeLease(const rpc::RevokeLeaseRequest& req);
  rpc::UpdateVersionResponse updateVersion(const rpc::UpdateVersionRequest& req);

  void setAdvertiseAddress(std::string address);
  void startHeartbeat();
  void stop();
  bool sendHeartbeat();

  const std::string& id() const { return id_; }
  const Config& config() const { return config_; }

 private:
  struct Prepared {
    MutationLock lock;
    LeaseInfo lease;
  };

  rpc::ResultCode prepareMutation(uint64_t handle, uint64_t version, Prepared* out);
  std::optional<std::string> forwardMutation(const rpc::ApplyMutationRequest& req, const std::vector<rpc::Replica>& secondaries);
  rpc::Chunkserver::Stub* stubFor(const std::string& address);
  void heartbeatLoop();

  Config config_;
  std::string id_;
  ChunkStore* store_;
  DataBuffer buffer_;
  LeaseTable leases_;
  std::unique_ptr<rpc::Master::Stub> master_;
  std::mutex stubs_mutex_;
  std::map<std::string, std::unique_ptr<rpc::Chunkserver::Stub>> stubs_;
  std::mutex advertise_mutex_;
  std::string advertise_;
  std::thread heartbeat_thread_;
  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;
  bool stopping_ = false;
  bool master_reachable_ = true;
};

}
