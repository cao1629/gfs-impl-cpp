#pragma once

#include <grpcpp/grpcpp.h>

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/clock.h"
#include "common/config.h"
#include "gfs.grpc.pb.h"

namespace gfs {

struct ChunkserverInfo {
  std::string id;
  std::string address;
  std::string rack;
  TimePoint last_heartbeat;
  bool alive = true;
};

class ChunkserverRegistry {
 public:
  explicit ChunkserverRegistry(const Config& config);

  bool Touch(const std::string& id, const std::string& address,
             const std::string& rack, TimePoint when);
  std::optional<ChunkserverInfo> Get(const std::string& id) const;
  std::optional<rpc::Replica> Resolve(const std::string& id) const;
  std::vector<ChunkserverInfo> Alive() const;
  std::vector<std::string> Sweep(TimePoint now);

  std::unique_ptr<rpc::Chunkserver::Stub> Stub(const std::string& id);
  std::unique_ptr<grpc::ClientContext> Context() const;

 private:
  const Config& config_;
  mutable std::mutex mutex_;
  std::map<std::string, ChunkserverInfo> servers_;
  std::map<std::string, std::shared_ptr<grpc::Channel>> channels_;
};

}  // namespace gfs
