#include "master/master.h"

#include <algorithm>
#include <filesystem>
#include <map>
#include <set>
#include <thread>

#include "common/logging.h"
#include "common/paths.h"
#include "master/checkpoint.h"

namespace gfs {

namespace fs = std::filesystem;

namespace {

grpc::Status Invalid(const std::string& message) {
  return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, message);
}
grpc::Status NotFound(const std::string& message) {
  return grpc::Status(grpc::StatusCode::NOT_FOUND, message);
}
grpc::Status AlreadyExists(const std::string& message) {
  return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, message);
}

bool CheckPath(const std::string& path, bool allow_root, grpc::Status* status) {
  if (!IsValidPath(path)) {
    *status = Invalid("malformed path: " + path);
    return false;
  }
  if (!allow_root && path == "/") {
    *status = Invalid("the root is not a file");
    return false;
  }
  return true;
}

}  // namespace

struct Master::LeaseCall {
  rpc::FindLeaseHolderRequest req;
  rpc::FindLeaseHolderResponse* resp;
  Done done;
  LockSet locks;
};

struct Master::SnapshotCall {
  rpc::SnapshotRequest req;
  rpc::SnapshotResponse* resp;
  Done done;
  LockSet locks;
  std::vector<uint64_t> handles;
};

Master::Master(Config config)
    : config_(std::move(config)),
      pool_(std::max<uint32_t>(2, config_.master_worker_threads)),
      timers_(pool_),
      oplog_(config_, state_.mutex),
      registry_(config_),
      leases_(config_, state_, registry_, oplog_) {}

Master::~Master() { Stop(); }

void Master::Start() {
  if (started_) return;
  started_ = true;
  Recover();
  sweep_ = std::make_unique<PeriodicTask>(config_.heartbeat_interval,
                                          [this] { SweepDead(); });
  gc_ =
      std::make_unique<PeriodicTask>(config_.gc_interval, [this] { GcPass(); });
  GFS_LOG_INFO << "master started with " << state_.files.Size() << " files and "
               << state_.chunks.Size() << " chunks, next handle "
               << state_.next_handle;
}

void Master::Stop() {
  if (!started_) return;
  started_ = false;
  gc_.reset();
  sweep_.reset();
  timers_.Stop();
  pool_.Stop();
  oplog_.Stop();
}

void Master::Post(std::function<void()> fn) { pool_.Post(std::move(fn)); }

void Master::Recover() {
  fs::create_directories(config_.data_dir);
  state::Checkpoint checkpoint;
  uint64_t checkpoint_number =
      Checkpointer::LoadLatest(config_.data_dir, &checkpoint);
  if (checkpoint_number > 0) {
    state_.Load(checkpoint);
    GFS_LOG_INFO << "loaded checkpoint " << checkpoint_number;
  }
  auto segments = OpLog::ListSegments(config_.data_dir);
  uint64_t first = checkpoint_number > 0 ? checkpoint_number : 1;
  size_t replayed = 0;
  for (uint64_t segment : segments) {
    if (segment < first) continue;
    ReplayedSegment replay = OpLog::ReadSegment(config_.data_dir, segment);
    for (const auto& record : replay.records) state_.Apply(record);
    replayed += replay.records.size();
    if (replay.torn_tail) {
      GFS_LOG_WARN << "segment " << segment
                   << " has a torn tail, truncating to " << replay.valid_bytes
                   << " bytes";
      OpLog::TruncateSegment(config_.data_dir, segment, replay.valid_bytes);
    }
  }
  state_.RecomputeRefcounts();
  uint64_t active = segments.empty() ? 1 : segments.back();
  active = std::max(active, first);
  GFS_LOG_INFO << "replayed " << replayed << " records, active segment "
               << active;
  oplog_.AddSink(std::make_unique<LocalFileSink>(config_.data_dir));
  oplog_.EnableCheckpoints(config_.data_dir,
                           [this] { return state_.ToCheckpoint(); });
  oplog_.Open(active);
}

uint64_t Master::Log(const state::LogRecord& record) {
  return oplog_.Append(record);
}

uint64_t Master::AllocHandle() {
  uint64_t handle = 0;
  uint64_t seq = 0;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    handle = state_.next_handle;
    state_.ApplyAllocHandle(handle);
    state::LogRecord record;
    record.mutable_alloc_handle()->set_handle(handle);
    seq = Log(record);
    state_.chunks.Create(handle, 1).pending = true;
  }
  oplog_.WaitFlushed(seq);
  return handle;
}

std::vector<std::string> Master::ChooseServers(size_t count) {
  auto servers = registry_.Alive();
  std::vector<std::pair<size_t, ChunkserverInfo>> ranked;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    for (auto& s : servers)
      ranked.emplace_back(state_.chunks.HeldCount(s.id), s);
  }
  std::stable_sort(
      ranked.begin(), ranked.end(),
      [](const auto& a, const auto& b) { return a.first < b.first; });
  std::vector<std::string> chosen;
  std::set<std::string> racks;
  for (const auto& [_, s] : ranked) {
    if (chosen.size() >= count) break;
    if (racks.count(s.rack) > 0) continue;
    racks.insert(s.rack);
    chosen.push_back(s.id);
  }
  for (const auto& [_, s] : ranked) {
    if (chosen.size() >= count) break;
    if (std::find(chosen.begin(), chosen.end(), s.id) == chosen.end())
      chosen.push_back(s.id);
  }
  return chosen;
}

std::vector<std::string> Master::CreateChunkOn(
    const std::vector<std::string>& ids, uint64_t handle, uint64_t version,
    uint64_t copy_from) {
  std::mutex results_mutex;
  std::vector<std::string> ok;
  std::vector<std::thread> threads;
  for (const auto& id : ids) {
    threads.emplace_back([&, id] {
      rpc::CreateChunkRequest req;
      req.set_handle(handle);
      req.set_version(version);
      req.set_copy_from(copy_from);
      rpc::CreateChunkResponse resp;
      auto stub = registry_.Stub(id);
      auto ctx = registry_.Context();
      bool success = stub && stub->CreateChunk(ctx.get(), req, &resp).ok() &&
                     resp.code() == rpc::OK;
      if (success) {
        std::lock_guard<std::mutex> lock(results_mutex);
        ok.push_back(id);
      } else {
        GFS_LOG_WARN << "CreateChunk " << handle << " failed on " << id;
      }
    });
  }
  for (auto& t : threads) t.join();
  return ok;
}

std::vector<rpc::Replica> Master::ResolveLocationsLocked(
    const ChunkMeta& meta) {
  std::vector<rpc::Replica> out;
  for (const auto& [id, _] : meta.locations) {
    if (auto replica = registry_.Resolve(id)) out.push_back(*replica);
  }
  return out;
}

void Master::FillChunkInfoLocked(rpc::ChunkInfo* info, uint64_t index,
                                 uint64_t handle, const ChunkMeta& meta) {
  info->set_index(index);
  info->set_handle(handle);
  info->set_version(meta.version);
  for (auto& replica : ResolveLocationsLocked(meta))
    *info->add_replicas() = replica;
}

void Master::GetClusterInfo(const rpc::GetClusterInfoRequest&,
                            rpc::GetClusterInfoResponse* resp, Done done) {
  resp->set_chunk_size(config_.chunk_size);
  resp->set_max_record_append_size(config_.EffectiveMaxRecordAppendSize());
  done(grpc::Status::OK);
}

void Master::Create(const rpc::CreateRequest& req, rpc::CreateResponse*,
                    Done done) {
  grpc::Status status;
  if (!CheckPath(req.path(), false, &status)) return done(status);
  LockSet locks =
      locks_.Acquire(LockTable::ForPath(req.path(), LockMode::kWrite));
  uint64_t seq = 0;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    if (state_.files.Exists(req.path()) || state_.files.IsDirectory(req.path()))
      status = AlreadyExists(req.path());
    else if (state_.files.HasFileAncestor(req.path()))
      status = Invalid("an ancestor of " + req.path() + " is a file");
    else {
      state_.ApplyCreate(req.path());
      state::LogRecord record;
      record.mutable_create()->set_path(req.path());
      seq = Log(record);
    }
  }
  if (seq > 0) oplog_.WaitFlushed(seq);
  locks.Release();
  done(status);
}

void Master::Open(const rpc::OpenRequest& req, rpc::OpenResponse* resp,
                  Done done) {
  grpc::Status status;
  if (!CheckPath(req.path(), false, &status)) return done(status);
  LockSet locks =
      locks_.Acquire(LockTable::ForPath(req.path(), LockMode::kRead));
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    const FileMeta* meta = state_.files.Find(req.path());
    if (meta == nullptr)
      status = NotFound(req.path());
    else
      resp->set_chunk_count(meta->chunks.size());
  }
  locks.Release();
  done(status);
}

void Master::Remove(const rpc::DeleteRequest& req, rpc::DeleteResponse*,
                    Done done) {
  grpc::Status status;
  if (!CheckPath(req.path(), false, &status)) return done(status);
  LockSet locks =
      locks_.Acquire(LockTable::ForPath(req.path(), LockMode::kWrite));
  uint64_t seq = 0;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    if (!state_.files.Exists(req.path()))
      status = NotFound(req.path());
    else if (IsHiddenPath(req.path())) {
      state_.ApplyRemove(req.path());
      state::LogRecord record;
      record.mutable_remove()->set_path(req.path());
      seq = Log(record);
    } else {
      int64_t stamp = UnixSeconds();
      std::string hidden = HiddenNameFor(req.path(), stamp);
      while (state_.files.Exists(hidden))
        hidden = HiddenNameFor(req.path(), ++stamp);
      state_.ApplyRename(req.path(), hidden);
      state::LogRecord record;
      record.mutable_rename()->set_source(req.path());
      record.mutable_rename()->set_target(hidden);
      seq = Log(record);
    }
  }
  if (seq > 0) oplog_.WaitFlushed(seq);
  locks.Release();
  done(status);
}

void Master::RevokeLeasesOn(const std::vector<uint64_t>& handles,
                            TimePoint* wait_until) {
  for (uint64_t handle : handles) {
    bool live = false;
    {
      std::lock_guard<std::mutex> lock(state_.mutex);
      const ChunkMeta* meta = state_.chunks.Find(handle);
      live = meta != nullptr && meta->lease.has_value();
    }
    if (!live) continue;
    RevokeResult result = leases_.Revoke(handle);
    if (result.had_lease && !result.acked && wait_until != nullptr) {
      TimePoint until =
          result.expiry + config_.lease_clock_skew_margin + Millis(1);
      *wait_until = std::max(*wait_until, until);
    }
  }
}

void Master::Rename(const rpc::RenameRequest& req, rpc::RenameResponse*,
                    Done done) {
  grpc::Status status;
  if (!CheckPath(req.source(), false, &status) ||
      !CheckPath(req.target(), false, &status))
    return done(status);
  if (req.source() == req.target()) return done(AlreadyExists(req.target()));
  if (IsAncestorOrSelf(req.source(), req.target()))
    return done(Invalid("target is inside the source"));
  LockSet locks = locks_.Acquire(LockTable::ForPaths(
      req.source(), LockMode::kWrite, req.target(), LockMode::kWrite));
  std::vector<uint64_t> handles;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    if (!state_.files.Exists(req.source()) &&
        !state_.files.IsDirectory(req.source()))
      status = NotFound(req.source());
    else if (state_.files.Exists(req.target()) ||
             state_.files.IsDirectory(req.target()))
      status = AlreadyExists(req.target());
    else if (state_.files.HasFileAncestor(req.target()))
      status = Invalid("an ancestor of " + req.target() + " is a file");
    else {
      for (auto& [_, meta] : state_.files.Subtree(req.source(), false))
        handles.insert(handles.end(), meta.chunks.begin(), meta.chunks.end());
    }
  }
  if (!status.ok()) {
    locks.Release();
    return done(status);
  }
  RevokeLeasesOn(handles, nullptr);
  uint64_t seq = 0;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    state_.ApplyRename(req.source(), req.target());
    state::LogRecord record;
    record.mutable_rename()->set_source(req.source());
    record.mutable_rename()->set_target(req.target());
    seq = Log(record);
  }
  oplog_.WaitFlushed(seq);
  locks.Release();
  done(grpc::Status::OK);
}

void Master::Snapshot(const rpc::SnapshotRequest& req,
                      rpc::SnapshotResponse* resp, Done done) {
  grpc::Status status;
  if (!CheckPath(req.source(), false, &status) ||
      !CheckPath(req.target(), false, &status))
    return done(status);
  if (req.source() == req.target()) return done(AlreadyExists(req.target()));
  if (IsAncestorOrSelf(req.source(), req.target()))
    return done(Invalid("target is inside the source"));
  auto call = std::make_shared<SnapshotCall>();
  call->req = req;
  call->resp = resp;
  call->done = std::move(done);
  call->locks = locks_.Acquire(LockTable::ForPaths(
      req.source(), LockMode::kWrite, req.target(), LockMode::kWrite));
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    if (!state_.files.Exists(req.source()) &&
        !state_.files.IsDirectory(req.source()))
      status = NotFound(req.source());
    else if (state_.files.Exists(req.target()) ||
             state_.files.IsDirectory(req.target()))
      status = AlreadyExists(req.target());
    else if (state_.files.HasFileAncestor(req.target()))
      status = Invalid("an ancestor of " + req.target() + " is a file");
    else {
      std::set<uint64_t> handles;
      for (auto& [_, meta] : state_.files.Subtree(req.source(), true))
        handles.insert(meta.chunks.begin(), meta.chunks.end());
      call->handles.assign(handles.begin(), handles.end());
    }
  }
  if (!status.ok()) {
    call->locks.Release();
    return call->done(status);
  }
  TimePoint wait_until = Now();
  RevokeLeasesOn(call->handles, &wait_until);
  if (wait_until > Now()) {
    GFS_LOG_INFO << "snapshot " << req.source() << " waiting for lease expiry";
    timers_.At(wait_until, [this, call] { SnapshotFinish(call); });
    return;
  }
  SnapshotFinish(call);
}

void Master::SnapshotFinish(std::shared_ptr<SnapshotCall> call) {
  uint64_t seq = 0;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    TimePoint t = Now();
    for (uint64_t handle : call->handles) {
      ChunkMeta* meta = state_.chunks.Find(handle);
      if (meta != nullptr && meta->lease && !leases_.IsValid(*meta, t))
        meta->lease.reset();
    }
    state_.ApplySnapshot(call->req.source(), call->req.target());
    state::LogRecord record;
    record.mutable_snapshot()->set_source(call->req.source());
    record.mutable_snapshot()->set_target(call->req.target());
    seq = Log(record);
  }
  oplog_.WaitFlushed(seq);
  call->locks.Release();
  GFS_LOG_INFO << "snapshot " << call->req.source() << " -> "
               << call->req.target() << " done";
  call->done(grpc::Status::OK);
}

void Master::FindMatchingFiles(const rpc::FindMatchingFilesRequest& req,
                               rpc::FindMatchingFilesResponse* resp,
                               Done done) {
  grpc::Status status;
  if (!CheckPath(req.directory(), true, &status)) return done(status);
  LockSet locks =
      locks_.Acquire(LockTable::ForPath(req.directory(), LockMode::kRead));
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    if (state_.files.Exists(req.directory()))
      status = Invalid(req.directory() + " is a file");
    else if (req.directory() != "/" &&
             !state_.files.IsDirectory(req.directory()))
      status = NotFound(req.directory());
    else {
      for (auto& entry :
           state_.files.List(req.directory(), req.include_hidden())) {
        rpc::DirEntry* out = resp->add_entries();
        out->set_name(entry.name);
        out->set_is_directory(entry.is_directory);
      }
    }
  }
  locks.Release();
  done(status);
}

void Master::FindLocation(const rpc::FindLocationRequest& req,
                          rpc::FindLocationResponse* resp, Done done) {
  grpc::Status status;
  if (!CheckPath(req.path(), false, &status)) return done(status);
  LockSet locks =
      locks_.Acquire(LockTable::ForPath(req.path(), LockMode::kRead));
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    const FileMeta* meta = state_.files.Find(req.path());
    if (meta == nullptr)
      status = NotFound(req.path());
    else {
      uint64_t count = std::max<uint64_t>(1, req.count());
      for (uint64_t index = req.first_index();
           index < meta->chunks.size() && index < req.first_index() + count;
           ++index) {
        const ChunkMeta* chunk = state_.chunks.Find(meta->chunks[index]);
        if (chunk == nullptr) continue;
        FillChunkInfoLocked(resp->add_chunks(), index, meta->chunks[index],
                            *chunk);
      }
    }
  }
  locks.Release();
  done(status);
}

uint64_t Master::CopyOnWrite(const std::string& path, uint64_t index,
                             uint64_t old_handle) {
  std::lock_guard<std::mutex> cow_lock(cow_mutex_);
  std::vector<std::string> holders;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    const FileMeta* file = state_.files.Find(path);
    if (file == nullptr || index >= file->chunks.size() ||
        file->chunks[index] != old_handle)
      return file != nullptr && index < file->chunks.size()
                 ? file->chunks[index]
                 : 0;
    const ChunkMeta* meta = state_.chunks.Find(old_handle);
    if (meta == nullptr) return 0;
    if (meta->refcount <= 1) return old_handle;
    for (const auto& [id, _] : meta->locations) {
      if (registry_.Resolve(id)) holders.push_back(id);
    }
  }
  uint64_t fresh = AllocHandle();
  std::vector<std::string> ok = CreateChunkOn(holders, fresh, 1, old_handle);
  uint64_t seq = 0;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    if (ok.size() < config_.min_replicas_for_write || ok.empty()) {
      state_.chunks.Erase(fresh);
      GFS_LOG_WARN << "copy-on-write of chunk " << old_handle
                   << " failed, not enough replicas";
      return 0;
    }
    TimePoint t = Now();
    for (const auto& id : ok) state_.chunks.AddLocation(fresh, id, t);
    state_.ApplyReplaceChunk(path, index, fresh);
    state::LogRecord record;
    record.mutable_replace_chunk()->set_path(path);
    record.mutable_replace_chunk()->set_index(index);
    record.mutable_replace_chunk()->set_handle(fresh);
    seq = Log(record);
  }
  oplog_.WaitFlushed(seq);
  GFS_LOG_INFO << "copy-on-write: " << path << " chunk " << index << " "
               << old_handle << " -> " << fresh;
  return fresh;
}

void Master::FindLeaseHolder(const rpc::FindLeaseHolderRequest& req,
                             rpc::FindLeaseHolderResponse* resp, Done done) {
  grpc::Status status;
  if (!CheckPath(req.path(), false, &status)) return done(status);
  auto call = std::make_shared<LeaseCall>();
  call->req = req;
  call->resp = resp;
  call->done = std::move(done);
  call->locks = locks_.Acquire(LockTable::ForPath(req.path(), LockMode::kRead));
  FindLeaseHolderAttempt(call);
}

void Master::FindLeaseHolderAttempt(std::shared_ptr<LeaseCall> call) {
  auto finish = [call](grpc::Status status) {
    call->locks.Release();
    call->done(status);
  };
  uint64_t handle = 0;
  bool shared = false;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    const FileMeta* file = state_.files.Find(call->req.path());
    if (file == nullptr) return finish(NotFound(call->req.path()));
    if (call->req.index() >= file->chunks.size())
      return finish(NotFound("no chunk " + std::to_string(call->req.index()) +
                             " in " + call->req.path()));
    handle = file->chunks[call->req.index()];
    const ChunkMeta* meta = state_.chunks.Find(handle);
    if (meta == nullptr) return finish(NotFound("chunk metadata missing"));
    shared = meta->refcount > 1;
  }
  if (shared) {
    handle = CopyOnWrite(call->req.path(), call->req.index(), handle);
    if (handle == 0) {
      call->resp->set_code(rpc::NO_REPLICAS);
      return finish(grpc::Status::OK);
    }
  }
  TimePoint wait_until;
  bool pending = false;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    ChunkMeta* meta = state_.chunks.Find(handle);
    if (meta == nullptr) return finish(NotFound("chunk metadata missing"));
    TimePoint t = Now();
    if (leases_.IsValid(*meta, t)) {
      call->resp->set_code(rpc::OK);
      call->resp->set_handle(handle);
      call->resp->set_version(meta->version);
      *call->resp->mutable_primary() = *registry_.Resolve(meta->lease->primary);
      for (auto& replica : ResolveLocationsLocked(*meta)) {
        if (replica.chunkserver_id() != meta->lease->primary)
          *call->resp->add_secondaries() = replica;
      }
      return finish(grpc::Status::OK);
    }
    if (leases_.IsPendingExpiry(*meta, t)) {
      pending = true;
      wait_until = leases_.WaitUntil(*meta);
    } else if (meta->lease) {
      meta->lease.reset();
    }
  }
  if (pending) {
    GFS_LOG_INFO << "chunk " << handle
                 << " lease pending expiry, deferring FindLeaseHolder";
    timers_.At(wait_until, [this, call] { FindLeaseHolderAttempt(call); });
    return;
  }
  GrantResult result = leases_.Grant(handle);
  call->resp->set_code(result.code);
  if (result.code == rpc::OK) {
    call->resp->set_handle(handle);
    call->resp->set_version(result.version);
    *call->resp->mutable_primary() = result.primary;
    for (auto& replica : result.secondaries)
      *call->resp->add_secondaries() = replica;
  }
  finish(grpc::Status::OK);
}

void Master::AddChunk(const rpc::AddChunkRequest& req,
                      rpc::AddChunkResponse* resp, Done done) {
  grpc::Status status;
  if (!CheckPath(req.path(), false, &status)) return done(status);
  LockSet locks =
      locks_.Acquire(LockTable::ForPath(req.path(), LockMode::kWrite));
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    const FileMeta* file = state_.files.Find(req.path());
    if (file == nullptr)
      status = NotFound(req.path());
    else if (req.index() < file->chunks.size()) {
      const ChunkMeta* meta = state_.chunks.Find(file->chunks[req.index()]);
      if (meta != nullptr)
        FillChunkInfoLocked(resp->mutable_chunk(), req.index(),
                            file->chunks[req.index()], *meta);
      resp->set_code(rpc::OK);
      status = grpc::Status::OK;
    } else if (req.index() > file->chunks.size())
      status = Invalid("chunk index " + std::to_string(req.index()) +
                       " would leave a hole");
    else
      status = grpc::Status(grpc::StatusCode::UNKNOWN, "create");
  }
  if (status.error_code() != grpc::StatusCode::UNKNOWN) {
    locks.Release();
    return done(status);
  }
  uint64_t handle = AllocHandle();
  std::vector<std::string> servers = ChooseServers(config_.replication_goal);
  std::vector<std::string> ok = servers.empty()
                                    ? std::vector<std::string>{}
                                    : CreateChunkOn(servers, handle, 1, 0);
  uint64_t seq = 0;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    if (ok.size() < config_.min_replicas_for_write || ok.empty()) {
      state_.chunks.Erase(handle);
      resp->set_code(rpc::NO_REPLICAS);
      GFS_LOG_WARN << "AddChunk " << req.path() << " index " << req.index()
                   << " failed: " << ok.size() << " of " << servers.size()
                   << " replicas created";
    } else {
      TimePoint t = Now();
      for (const auto& id : ok) state_.chunks.AddLocation(handle, id, t);
      state_.ApplyAddChunk(req.path(), req.index(), handle);
      state::LogRecord record;
      record.mutable_add_chunk()->set_path(req.path());
      record.mutable_add_chunk()->set_index(req.index());
      record.mutable_add_chunk()->set_handle(handle);
      seq = Log(record);
      resp->set_code(rpc::OK);
      FillChunkInfoLocked(resp->mutable_chunk(), req.index(), handle,
                          *state_.chunks.Find(handle));
    }
  }
  if (seq > 0) oplog_.WaitFlushed(seq);
  locks.Release();
  done(grpc::Status::OK);
}

void Master::HeartBeat(const rpc::HeartBeatRequest& req,
                       rpc::HeartBeatResponse* resp, Done done) {
  TimePoint t = Now();
  registry_.Touch(req.chunkserver_id(), req.address(), req.rack(), t);
  std::vector<uint64_t> regrants;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    std::set<uint64_t> known = state_.chunks.HeldBy(req.chunkserver_id());
    std::set<uint64_t> reported;
    for (const auto& report : req.chunks()) {
      ChunkMeta* meta = state_.chunks.Find(report.handle());
      if (meta == nullptr) {
        resp->add_delete_handles(report.handle());
        continue;
      }
      if (meta->granting) {
        if (report.version() + 1 < meta->version) {
          resp->add_delete_handles(report.handle());
          state_.chunks.RemoveLocation(report.handle(), req.chunkserver_id());
          continue;
        }
        reported.insert(report.handle());
        state_.chunks.AddLocation(report.handle(), req.chunkserver_id(), t);
        continue;
      }
      if (report.version() < meta->version) {
        resp->add_delete_handles(report.handle());
        if (state_.chunks.RemoveLocation(report.handle(),
                                         req.chunkserver_id()) &&
            leases_.IsValid(*meta, t))
          regrants.push_back(report.handle());
        GFS_LOG_WARN << "chunk " << report.handle() << " on "
                     << req.chunkserver_id() << " is stale: version "
                     << report.version() << " < " << meta->version;
        continue;
      }
      if (report.version() > meta->version) {
        GFS_LOG_WARN << "chunk " << report.handle() << " reported at version "
                     << report.version() << " above ours " << meta->version
                     << ", adopting";
        meta->version = report.version();
        state::LogRecord record;
        record.mutable_bump_version()->set_handle(report.handle());
        record.mutable_bump_version()->set_version(report.version());
        Log(record);
      }
      reported.insert(report.handle());
      if (state_.chunks.AddLocation(report.handle(), req.chunkserver_id(), t) &&
          leases_.IsValid(*meta, t))
        regrants.push_back(report.handle());
    }
    for (uint64_t handle : known) {
      if (reported.count(handle) > 0) continue;
      ChunkMeta* meta = state_.chunks.Find(handle);
      if (meta == nullptr) continue;
      auto since = meta->locations.find(req.chunkserver_id());
      if (since != meta->locations.end() &&
          t - since->second < config_.heartbeat_interval * 2)
        continue;
      state_.chunks.RemoveLocation(handle, req.chunkserver_id());
      if (leases_.IsValid(*meta, t)) regrants.push_back(handle);
    }
    for (uint64_t handle : req.corrupt()) {
      ChunkMeta* meta = state_.chunks.Find(handle);
      resp->add_delete_handles(handle);
      if (meta == nullptr) continue;
      if (state_.chunks.RemoveLocation(handle, req.chunkserver_id()))
        GFS_LOG_WARN << "chunk " << handle << " on " << req.chunkserver_id()
                     << " reported corrupt";
      if (leases_.IsValid(*meta, t)) regrants.push_back(handle);
    }
    for (uint64_t handle : req.lease_extension_requests()) {
      ChunkMeta* meta = state_.chunks.Find(handle);
      if (meta == nullptr ||
          !leases_.ExtendLocked(*meta, req.chunkserver_id(), t))
        continue;
      rpc::LeaseExtension* extension = resp->add_extended();
      extension->set_handle(handle);
      extension->set_lease_ms(
          static_cast<uint64_t>(config_.lease_duration.count()));
    }
  }
  for (uint64_t handle : regrants) leases_.RequestRegrant(handle, pool_);
  done(grpc::Status::OK);
}

void Master::SweepDead() {
  TimePoint t = Now();
  std::vector<std::string> dead = registry_.Sweep(t);
  if (dead.empty()) return;
  std::vector<uint64_t> regrants;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    for (const auto& id : dead) {
      for (uint64_t handle : state_.chunks.HeldBy(id)) {
        state_.chunks.RemoveLocation(handle, id);
        ChunkMeta* meta = state_.chunks.Find(handle);
        if (meta != nullptr && leases_.IsValid(*meta, t))
          regrants.push_back(handle);
      }
    }
  }
  for (uint64_t handle : regrants) leases_.RequestRegrant(handle, pool_);
}

void Master::GcPass() {
  int64_t now_seconds = UnixSeconds();
  int64_t retention_seconds = config_.deleted_file_retention.count() / 1000;
  std::vector<std::string> expired;
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    state_.files.ForEach([&](const std::string& path, const FileMeta&) {
      int64_t stamp = 0;
      if (ParseHiddenName(path, &stamp, nullptr) &&
          stamp + retention_seconds <= now_seconds)
        expired.push_back(path);
    });
  }
  uint64_t last_seq = 0;
  for (const auto& path : expired) {
    LockSet locks = locks_.Acquire(LockTable::ForPath(path, LockMode::kWrite));
    std::lock_guard<std::mutex> lock(state_.mutex);
    if (!state_.files.Exists(path)) continue;
    state_.ApplyRemove(path);
    state::LogRecord record;
    record.mutable_remove()->set_path(path);
    last_seq = Log(record);
    GFS_LOG_INFO << "gc removed " << path;
  }
  {
    std::lock_guard<std::mutex> lock(state_.mutex);
    std::set<uint64_t> referenced;
    state_.files.ForEach([&](const std::string&, const FileMeta& meta) {
      referenced.insert(meta.chunks.begin(), meta.chunks.end());
    });
    std::vector<uint64_t> orphans;
    state_.chunks.ForEach([&](uint64_t handle, const ChunkMeta& meta) {
      if (!meta.pending && referenced.count(handle) == 0)
        orphans.push_back(handle);
    });
    for (uint64_t handle : orphans) {
      state_.ApplyDropChunk(handle);
      state::LogRecord record;
      record.mutable_drop_chunk()->set_handle(handle);
      last_seq = Log(record);
      GFS_LOG_INFO << "gc dropped orphan chunk " << handle;
    }
  }
  if (last_seq > 0) oplog_.WaitFlushed(last_seq);
}

}  // namespace gfs
