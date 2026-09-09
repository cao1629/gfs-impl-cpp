#include "master/chunkserver_registry.h"

#include "common/logging.h"

namespace gfs {

ChunkserverRegistry::ChunkserverRegistry(const Config& config) : config_(config) {}

bool ChunkserverRegistry::touch(const std::string& id, const std::string& address, const std::string& rack, TimePoint when) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto [it, inserted] = servers_.try_emplace(id);
  ChunkserverInfo& info = it->second;
  bool newly_alive = inserted || !info.alive;
  if (inserted) GFS_LOG_INFO << "chunkserver " << id << " joined at " << address << " rack " << rack;
  else if (!info.alive) GFS_LOG_INFO << "chunkserver " << id << " is back at " << address;
  else if (info.address != address) GFS_LOG_INFO << "chunkserver " << id << " moved to " << address;
  info.id = id;
  info.address = address;
  info.rack = rack;
  info.last_heartbeat = when;
  info.alive = true;
  return newly_alive;
}

std::optional<ChunkserverInfo> ChunkserverRegistry::get(const std::string& id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = servers_.find(id);
  if (it == servers_.end()) return std::nullopt;
  return it->second;
}

std::optional<rpc::Replica> ChunkserverRegistry::resolve(const std::string& id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = servers_.find(id);
  if (it == servers_.end() || !it->second.alive) return std::nullopt;
  rpc::Replica replica;
  replica.set_chunkserver_id(id);
  replica.set_address(it->second.address);
  replica.set_rack(it->second.rack);
  return replica;
}

std::vector<ChunkserverInfo> ChunkserverRegistry::alive() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<ChunkserverInfo> out;
  for (const auto& [_, info] : servers_) {
    if (info.alive) out.push_back(info);
  }
  return out;
}

std::vector<std::string> ChunkserverRegistry::sweep(TimePoint now) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::string> dead;
  for (auto& [id, info] : servers_) {
    if (info.alive && now - info.last_heartbeat > config_.chunkserver_dead_timeout) {
      info.alive = false;
      dead.push_back(id);
      GFS_LOG_WARN << "chunkserver " << id << " at " << info.address << " declared dead";
    }
  }
  return dead;
}

std::unique_ptr<rpc::Chunkserver::Stub> ChunkserverRegistry::stub(const std::string& id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = servers_.find(id);
  if (it == servers_.end()) return nullptr;
  auto& channel = channels_[it->second.address];
  if (!channel) channel = grpc::CreateChannel(it->second.address, grpc::InsecureChannelCredentials());
  return rpc::Chunkserver::NewStub(channel);
}

std::unique_ptr<grpc::ClientContext> ChunkserverRegistry::context() const {
  auto ctx = std::make_unique<grpc::ClientContext>();
  ctx->set_deadline(std::chrono::system_clock::now() + config_.rpc_deadline);
  return ctx;
}

}
