#pragma once

#include <grpcpp/grpcpp.h>

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

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

  rpc::PushDataResponse PushData(
      grpc::ServerReader<rpc::PushDataRequest>* reader);
  rpc::ReadResponse Read(const rpc::ReadRequest& req);
  rpc::WriteResponse Write(const rpc::WriteRequest& req);
  rpc::RecordAppendResponse RecordAppend(const rpc::RecordAppendRequest& req);
  rpc::GetChunkLengthResponse GetChunkLength(
      const rpc::GetChunkLengthRequest& req);
  rpc::ApplyMutationResponse ApplyMutation(
      const rpc::ApplyMutationRequest& req);
  rpc::CreateChunkResponse CreateChunk(const rpc::CreateChunkRequest& req);
  rpc::GrantLeaseResponse GrantLease(const rpc::GrantLeaseRequest& req);
  rpc::RevokeLeaseResponse RevokeLease(const rpc::RevokeLeaseRequest& req);
  rpc::UpdateVersionResponse UpdateVersion(
      const rpc::UpdateVersionRequest& req);

  void SetAdvertiseAddress(std::string address);
  void StartHeartbeat();
  void Stop();
  bool SendHeartbeat();

  const std::string& Id() const { return id_; }
  // NOLINTNEXTLINE(readability-identifier-naming)
  const Config& config() const { return config_; }

 private:
  struct Prepared {
    MutationLock lock;
    LeaseInfo lease;
  };

  rpc::ResultCode PrepareMutation(uint64_t handle, uint64_t version,
                                  Prepared* out);
  std::optional<std::string> ForwardMutation(
      const rpc::ApplyMutationRequest& req,
      const std::vector<rpc::Replica>& secondaries);
  rpc::Chunkserver::Stub* StubFor(const std::string& address);
  void HeartbeatLoop();

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

}  // namespace gfs
