#include "master/lease_manager.h"

#include <algorithm>
#include <thread>

#include "common/logging.h"

namespace gfs {

LeaseManager::LeaseManager(const Config& config, MasterState& state, ChunkserverRegistry& registry, OpLog& oplog)
    : config_(config), state_(state), registry_(registry), oplog_(oplog) {}

bool LeaseManager::isValid(const ChunkMeta& meta, TimePoint now) const {
  if (!meta.lease || meta.lease->revoked) return false;
  if (now > meta.lease->expiry + config_.lease_clock_skew_margin) return false;
  return meta.locations.count(meta.lease->primary) > 0;
}

bool LeaseManager::isPendingExpiry(const ChunkMeta& meta, TimePoint now) const {
  if (!meta.lease) return false;
  if (now > meta.lease->expiry + config_.lease_clock_skew_margin) return false;
  return meta.lease->revoked || meta.locations.count(meta.lease->primary) == 0;
}

TimePoint LeaseManager::waitUntil(const ChunkMeta& meta) const {
  return meta.lease->expiry + config_.lease_clock_skew_margin + Millis(1);
}

bool LeaseManager::extendLocked(ChunkMeta& meta, const std::string& primary, TimePoint now) const {
  if (!isValid(meta, now) || meta.lease->primary != primary) return false;
  meta.lease->expiry = now + config_.lease_duration;
  return true;
}

void LeaseManager::beginHandle(uint64_t handle) {
  std::unique_lock<std::mutex> lock(busy_mutex_);
  busy_cv_.wait(lock, [&] { return busy_.count(handle) == 0; });
  busy_.insert(handle);
}

void LeaseManager::endHandle(uint64_t handle) {
  {
    std::lock_guard<std::mutex> lock(busy_mutex_);
    busy_.erase(handle);
  }
  busy_cv_.notify_all();
}

LeaseManager::Attempt LeaseManager::prepareLocked(uint64_t handle, TimePoint now) {
  Attempt attempt;
  ChunkMeta* meta = state_.chunks.find(handle);
  if (meta == nullptr) return attempt;
  std::vector<std::string> live;
  for (const auto& [id, _] : meta->locations) {
    if (registry_.resolve(id)) live.push_back(id);
  }
  if (live.size() < config_.min_replicas_for_write || live.empty()) return attempt;
  std::string primary = live.front();
  if (meta->lease) {
    for (const auto& id : live) {
      if (id == meta->lease->primary) primary = id;
    }
  }
  attempt.possible = true;
  attempt.primary = primary;
  for (const auto& id : live) {
    if (id != primary) attempt.secondaries.push_back(id);
  }
  attempt.version = meta->version + 1;
  meta->granting = true;
  (void)now;
  return attempt;
}

GrantResult LeaseManager::grantSerialized(uint64_t handle) {
  GrantResult result;
  auto clearGranting = [&] {
    std::lock_guard<std::mutex> lock(state_.mutex);
    ChunkMeta* meta = state_.chunks.find(handle);
    if (meta != nullptr) meta->granting = false;
  };
  for (int round = 0; round < 8; ++round) {
    Attempt attempt;
    {
      std::lock_guard<std::mutex> lock(state_.mutex);
      attempt = prepareLocked(handle, now());
    }
    if (!attempt.possible) {
      clearGranting();
      result.code = rpc::NO_REPLICAS;
      return result;
    }

    std::vector<rpc::Replica> secondary_replicas;
    for (const auto& id : attempt.secondaries) {
      if (auto replica = registry_.resolve(id)) secondary_replicas.push_back(*replica);
    }
    std::mutex results_mutex;
    std::set<std::string> failed;
    std::vector<std::thread> threads;
    threads.emplace_back([&] {
      rpc::GrantLeaseRequest req;
      req.set_handle(handle);
      req.set_version(attempt.version);
      req.set_lease_ms(static_cast<uint64_t>(config_.lease_duration.count()));
      for (const auto& r : secondary_replicas) *req.add_secondaries() = r;
      rpc::GrantLeaseResponse resp;
      auto stub = registry_.stub(attempt.primary);
      auto ctx = registry_.context();
      bool ok = stub && stub->GrantLease(ctx.get(), req, &resp).ok() && resp.code() == rpc::OK;
      if (!ok) {
        std::lock_guard<std::mutex> lock(results_mutex);
        failed.insert(attempt.primary);
      }
    });
    for (const auto& id : attempt.secondaries) {
      threads.emplace_back([&, id] {
        rpc::UpdateVersionRequest req;
        req.set_handle(handle);
        req.set_version(attempt.version);
        rpc::UpdateVersionResponse resp;
        auto stub = registry_.stub(id);
        auto ctx = registry_.context();
        bool ok = stub && stub->UpdateVersion(ctx.get(), req, &resp).ok() && resp.code() == rpc::OK;
        if (!ok) {
          std::lock_guard<std::mutex> lock(results_mutex);
          failed.insert(id);
        }
      });
    }
    for (auto& t : threads) t.join();

    uint64_t seq = 0;
    {
      std::lock_guard<std::mutex> lock(state_.mutex);
      ChunkMeta* meta = state_.chunks.find(handle);
      if (meta == nullptr) {
        result.code = rpc::NO_SUCH_CHUNK;
        return result;
      }
      for (const auto& id : failed) {
        GFS_LOG_WARN << "chunk " << handle << " replica " << id << " did not ack version " << attempt.version << ", dropping it";
        state_.chunks.removeLocation(handle, id);
      }
      if (failed.count(attempt.primary) > 0) continue;
      meta->version = std::max(meta->version, attempt.version);
      state::LogRecord record;
      record.mutable_bump_version()->set_handle(handle);
      record.mutable_bump_version()->set_version(meta->version);
      seq = oplog_.append(record);
      meta->granting = false;
      meta->lease = Lease{attempt.primary, now() + config_.lease_duration, false};
      result.code = rpc::OK;
      result.version = meta->version;
      result.primary = *registry_.resolve(attempt.primary);
      for (const auto& id : attempt.secondaries) {
        if (failed.count(id) == 0) {
          if (auto replica = registry_.resolve(id)) result.secondaries.push_back(*replica);
        }
      }
    }
    oplog_.waitFlushed(seq);
    GFS_LOG_INFO << "chunk " << handle << " lease granted to " << attempt.primary << " at version " << result.version;
    return result;
  }
  clearGranting();
  result.code = rpc::NO_REPLICAS;
  return result;
}

GrantResult LeaseManager::grant(uint64_t handle) {
  beginHandle(handle);
  GrantResult result = grantSerialized(handle);
  endHandle(handle);
  return result;
}

void LeaseManager::requestRegrant(uint64_t handle, WorkerPool& pool) {
  {
    std::lock_guard<std::mutex> lock(busy_mutex_);
    if (!regrant_pending_.insert(handle).second) return;
  }
  pool.post([this, handle] { regrant(handle); });
}

void LeaseManager::regrant(uint64_t handle) {
  beginHandle(handle);
  {
    std::lock_guard<std::mutex> lock(busy_mutex_);
    regrant_pending_.erase(handle);
  }
  bool needed = false;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    ChunkMeta* meta = state_.chunks.find(handle);
    needed = meta != nullptr && isValid(*meta, now());
  }
  if (needed) {
    GrantResult result = grantSerialized(handle);
    if (result.code != rpc::OK) GFS_LOG_WARN << "regrant of chunk " << handle << " failed with code " << result.code;
  }
  endHandle(handle);
}

RevokeResult LeaseManager::revoke(uint64_t handle) {
  RevokeResult result;
  std::string primary;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    ChunkMeta* meta = state_.chunks.find(handle);
    if (meta == nullptr || !meta->lease) return result;
    if (now() > meta->lease->expiry + config_.lease_clock_skew_margin) {
      meta->lease.reset();
      return result;
    }
    result.had_lease = true;
    result.expiry = meta->lease->expiry;
    meta->lease->revoked = true;
    primary = meta->lease->primary;
  }
  rpc::RevokeLeaseRequest req;
  req.set_handle(handle);
  rpc::RevokeLeaseResponse resp;
  auto stub = registry_.stub(primary);
  auto ctx = registry_.context();
  bool ok = stub && stub->RevokeLease(ctx.get(), req, &resp).ok() && resp.code() == rpc::OK;
  std::lock_guard<std::mutex> lock(state_.mutex);
  ChunkMeta* meta = state_.chunks.find(handle);
  if (ok && meta != nullptr && meta->lease && meta->lease->primary == primary) {
    meta->lease.reset();
    result.acked = true;
  }
  if (!ok) GFS_LOG_WARN << "chunk " << handle << " revoke not acked by " << primary << ", waiting for expiry";
  return result;
}

}
