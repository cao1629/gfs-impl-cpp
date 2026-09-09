#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

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

  bool touch(const std::string& id, const std::string& address, const std::string& rack, TimePoint when);
  std::optional<ChunkserverInfo> get(const std::string& id) const;
  std::optional<rpc::Replica> resolve(const std::string& id) const;
  std::vector<ChunkserverInfo> alive() const;
  std::vector<std::string> sweep(TimePoint now);

  std::unique_ptr<rpc::Chunkserver::Stub> stub(const std::string& id);
  std::unique_ptr<grpc::ClientContext> context() const;

 private:
  const Config& config_;
  mutable std::mutex mutex_;
  std::map<std::string, ChunkserverInfo> servers_;
  std::map<std::string, std::shared_ptr<grpc::Channel>> channels_;
};

}
