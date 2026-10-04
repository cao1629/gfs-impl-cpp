#include "client/gfs_client.h"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <thread>

#include "client/location_cache.h"
#include "common/clock.h"
#include "common/distance.h"
#include "common/ids.h"
#include "common/paths.h"
#include "gfs.grpc.pb.h"

namespace gfs {

namespace {

constexpr uint32_t kLocationBatch = 4;

Status FromGrpc(const grpc::Status& status) {
  if (status.ok()) return Status::Ok();
  switch (status.error_code()) {
    case grpc::StatusCode::NOT_FOUND:
      return Status::Error(ErrorCode::kNotFound, status.error_message());
    case grpc::StatusCode::ALREADY_EXISTS:
      return Status::Error(ErrorCode::kAlreadyExists, status.error_message());
    case grpc::StatusCode::INVALID_ARGUMENT:
      return Status::Error(ErrorCode::kInvalidArgument, status.error_message());
    case grpc::StatusCode::UNAVAILABLE:
    case grpc::StatusCode::DEADLINE_EXCEEDED:
      return Status::Error(ErrorCode::kUnavailable, status.error_message());
    default:
      return Status::Error(ErrorCode::kFailed, status.error_message());
  }
}

Status FromResultCode(rpc::ResultCode code, const std::string& where) {
  std::string name = rpc::ResultCode_Name(code);
  switch (code) {
    case rpc::OK:
      return Status::Ok();
    case rpc::STALE_VERSION:
    case rpc::NOT_PRIMARY:
    case rpc::LEASE_EXPIRED:
    case rpc::NO_SUCH_CHUNK:
      return Status::Error(ErrorCode::kStale, where + ": " + name);
    case rpc::OUT_OF_RANGE:
      return Status::Error(ErrorCode::kInvalidArgument, where + ": " + name);
    case rpc::NO_REPLICAS:
    case rpc::CHECKSUM_MISMATCH:
    case rpc::DATA_MISSING:
      return Status::Error(ErrorCode::kUnavailable, where + ": " + name);
    default:
      return Status::Error(ErrorCode::kFailed, where + ": " + name);
  }
}

void SetDeadline(grpc::ClientContext* ctx, Millis timeout) {
  ctx->set_deadline(std::chrono::system_clock::now() + timeout);
}

void SleepFor(Millis duration) {
  if (duration.count() > 0) std::this_thread::sleep_for(duration);
}

bool IsRetryableLater(const Status& status) {
  return status.code == ErrorCode::kUnavailable ||
         status.code == ErrorCode::kStale || status.code == ErrorCode::kFailed;
}

}  // namespace

class Client::Impl {
 public:
  explicit Impl(Config config)
      : config_(std::move(config)),
        client_id_(RandomHexId()),
        replica_seed_(std::hash<std::string>{}(client_id_)),
        master_(rpc::Master::NewStub(grpc::CreateChannel(
            config_.master_address, grpc::InsecureChannelCredentials()))),
        locations_(config_.client_location_cache_ttl),
        leases_(config_.client_location_cache_ttl) {}

  Status Create(const std::string& path) {
    rpc::CreateRequest req;
    req.set_path(path);
    rpc::CreateResponse resp;
    return FromGrpc(CallMaster(&rpc::Master::Stub::Create, req, &resp));
  }

  Status Remove(const std::string& path) {
    rpc::DeleteRequest req;
    req.set_path(path);
    rpc::DeleteResponse resp;
    Status status =
        FromGrpc(CallMaster(&rpc::Master::Stub::Delete, req, &resp));
    ForgetFile(path);
    return status;
  }

  Status Rename(const std::string& source, const std::string& target) {
    rpc::RenameRequest req;
    req.set_source(source);
    req.set_target(target);
    rpc::RenameResponse resp;
    Status status =
        FromGrpc(CallMaster(&rpc::Master::Stub::Rename, req, &resp));
    ForgetFile(source);
    ForgetFile(target);
    return status;
  }

  Status Snapshot(const std::string& source, const std::string& target) {
    rpc::SnapshotRequest req;
    req.set_source(source);
    req.set_target(target);
    rpc::SnapshotResponse resp;
    Status status = FromGrpc(CallMaster(&rpc::Master::Stub::Snapshot, req,
                                        &resp, LeaseWaitDeadline()));
    ForgetFile(source);
    return status;
  }

  Status List(const std::string& directory, std::vector<DirEntry>* entries,
              bool include_hidden) {
    rpc::FindMatchingFilesRequest req;
    req.set_directory(directory);
    req.set_include_hidden(include_hidden);
    rpc::FindMatchingFilesResponse resp;
    Status status =
        FromGrpc(CallMaster(&rpc::Master::Stub::FindMatchingFiles, req, &resp));
    if (!status.ok()) return status;
    entries->clear();
    for (const auto& e : resp.entries())
      entries->push_back({e.name(), e.is_directory()});
    return Status::Ok();
  }

  Status Open(const std::string& path, FileInfo* info) {
    rpc::OpenRequest req;
    req.set_path(path);
    rpc::OpenResponse resp;
    Status status = FromGrpc(CallMaster(&rpc::Master::Stub::Open, req, &resp));
    if (!status.ok()) return status;
    if (info) info->chunk_count = resp.chunk_count();
    return Status::Ok();
  }

  Status Length(const std::string& path, uint64_t* length) {
    Status status = EnsureClusterInfo();
    if (!status.ok()) return status;
    FileInfo info;
    status = Open(path, &info);
    if (!status.ok()) return status;
    if (info.chunk_count == 0) {
      *length = 0;
      return Status::Ok();
    }
    uint64_t last = info.chunk_count - 1;
    for (int attempt = 0; attempt < 2; ++attempt) {
      CachedChunk chunk;
      bool missing = false;
      status = Locate(path, last, &chunk, &missing);
      if (!status.ok()) return status;
      if (missing) {
        *length = last * chunk_size_;
        return Status::Ok();
      }
      size_t n = chunk.replicas.size();
      for (size_t k = 0; k < n; ++k) {
        const auto& replica = chunk.replicas[(replica_seed_ + k) % n];
        rpc::GetChunkLengthRequest req;
        req.set_handle(chunk.handle);
        rpc::GetChunkLengthResponse resp;
        grpc::ClientContext ctx;
        SetDeadline(&ctx, config_.client_rpc_deadline);
        auto s =
            Chunkserver(replica.address())->GetChunkLength(&ctx, req, &resp);
        if (!s.ok() || resp.code() != rpc::OK) continue;
        *length = last * chunk_size_ + resp.length();
        return Status::Ok();
      }
      locations_.Invalidate(path, last);
    }
    return Status::Error(
        ErrorCode::kUnavailable,
        "no replica could report the length of the last chunk");
  }

  Status Read(const std::string& path, uint64_t offset, uint64_t length,
              std::string* data) {
    data->clear();
    if (length == 0) return Status::Ok();
    Status status = EnsureClusterInfo();
    if (!status.ok()) return status;
    uint64_t pos = offset;
    uint64_t end = offset + length;
    while (pos < end) {
      uint64_t index = pos / chunk_size_;
      uint64_t in_chunk = pos % chunk_size_;
      uint64_t want = std::min(end - pos, chunk_size_ - in_chunk);
      std::string piece;
      bool eof = false;
      status = ReadChunk(path, index, in_chunk, want, &piece, &eof);
      if (!status.ok()) return status;
      if (eof) break;
      data->append(piece);
      pos += piece.size();
      if (piece.size() < want) break;
    }
    return Status::Ok();
  }

  Status Write(const std::string& path, uint64_t offset,
               std::string_view data) {
    if (data.empty()) return Status::Ok();
    Status status = EnsureClusterInfo();
    if (!status.ok()) return status;
    uint64_t pos = 0;
    while (pos < data.size()) {
      uint64_t absolute = offset + pos;
      uint64_t index = absolute / chunk_size_;
      uint64_t in_chunk = absolute % chunk_size_;
      uint64_t len =
          std::min<uint64_t>(data.size() - pos, chunk_size_ - in_chunk);
      status = WriteChunk(path, index, in_chunk, data.substr(pos, len));
      if (!status.ok()) return status;
      pos += len;
    }
    return Status::Ok();
  }

  Status RecordAppend(const std::string& path, std::string_view data,
                      uint64_t* offset) {
    Status status = EnsureClusterInfo();
    if (!status.ok()) return status;
    if (data.size() > max_append_) {
      return Status::Error(ErrorCode::kInvalidArgument,
                           "record of " + std::to_string(data.size()) +
                               " bytes exceeds the append limit of " +
                               std::to_string(max_append_));
    }
    FileInfo info;
    status = Open(path, &info);
    if (!status.ok()) return status;
    uint64_t index = info.chunk_count == 0 ? 0 : info.chunk_count - 1;
    if (info.chunk_count == 0) {
      status = EnsureChunk(path, 0);
      if (!status.ok()) return status;
    }
    Status last =
        Status::Error(ErrorCode::kFailed, "record append retries exhausted");
    int outer = 0;
    bool ensured = false;
    while (outer <= static_cast<int>(config_.mutation_retry_outer)) {
      CachedLease lease;
      bool from_cache = false;
      status = FindLease(path, index, &lease, &from_cache);
      if (status.code == ErrorCode::kNotFound && !ensured) {
        status = EnsureChunk(path, index);
        if (status.code == ErrorCode::kNotFound ||
            status.code == ErrorCode::kInvalidArgument)
          return status;
        ensured = true;
        continue;
      }
      if (!status.ok()) {
        if (!IsRetryableLater(status)) return status;
        last = status;
        ++outer;
        SleepFor(config_.EffectiveOuterRetryDelay());
        continue;
      }
      bool next_chunk = false;
      uint64_t in_chunk = 0;
      Status attempt = TryAppend(lease, data, &in_chunk, &next_chunk);
      if (attempt.ok()) {
        if (offset) *offset = index * chunk_size_ + in_chunk;
        return Status::Ok();
      }
      if (next_chunk) {
        status = EnsureChunk(path, index + 1);
        if (!status.ok() && !IsRetryableLater(status)) return status;
        if (status.ok()) {
          ++index;
          ensured = true;
          outer = 0;
        }
        continue;
      }
      if (attempt.code == ErrorCode::kInvalidArgument) return attempt;
      last = attempt;
      leases_.Invalidate(path, index);
      if (from_cache) continue;
      ++outer;
      SleepFor(config_.EffectiveOuterRetryDelay());
    }
    return last;
  }

  uint64_t ChunkSize() {
    EnsureClusterInfo();
    return chunk_size_;
  }

  const std::string& ClientId() const { return client_id_; }

 private:
  template <typename Req, typename Resp>
  grpc::Status CallMaster(grpc::Status (rpc::Master::Stub::*method)(
                              grpc::ClientContext*, const Req&, Resp*),
                          const Req& req, Resp* resp, Millis timeout) {
    grpc::ClientContext ctx;
    SetDeadline(&ctx, timeout);
    return (master_.get()->*method)(&ctx, req, resp);
  }

  template <typename Req, typename Resp>
  grpc::Status CallMaster(grpc::Status (rpc::Master::Stub::*method)(
                              grpc::ClientContext*, const Req&, Resp*),
                          const Req& req, Resp* resp) {
    return CallMaster(method, req, resp, config_.client_rpc_deadline);
  }

  rpc::Chunkserver::Stub* Chunkserver(const std::string& address) {
    std::lock_guard<std::mutex> lock(stubs_mu_);
    auto& stub = chunkservers_[address];
    if (!stub) {
      grpc::ChannelArguments args;
      args.SetMaxReceiveMessageSize(-1);
      args.SetMaxSendMessageSize(-1);
      stub = rpc::Chunkserver::NewStub(grpc::CreateCustomChannel(
          address, grpc::InsecureChannelCredentials(), args));
    }
    return stub.get();
  }

  Status EnsureClusterInfo() {
    std::lock_guard<std::mutex> lock(info_mu_);
    if (info_loaded_) return Status::Ok();
    rpc::GetClusterInfoRequest req;
    rpc::GetClusterInfoResponse resp;
    Status status =
        FromGrpc(CallMaster(&rpc::Master::Stub::GetClusterInfo, req, &resp));
    if (!status.ok()) return status;
    if (resp.chunk_size() == 0)
      return Status::Error(ErrorCode::kFailed,
                           "master reported a chunk size of zero");
    chunk_size_ = resp.chunk_size();
    max_append_ = resp.max_record_append_size() == 0
                      ? chunk_size_ / 4
                      : resp.max_record_append_size();
    info_loaded_ = true;
    return Status::Ok();
  }

  void ForgetFile(const std::string& path) {
    locations_.InvalidateFile(path);
    leases_.InvalidateFile(path);
  }

  Status Locate(const std::string& path, uint64_t index, CachedChunk* out,
                bool* missing) {
    *missing = false;
    if (auto cached = locations_.Get(path, index)) {
      *out = *cached;
      return Status::Ok();
    }
    rpc::FindLocationRequest req;
    req.set_path(path);
    req.set_first_index(index);
    req.set_count(kLocationBatch);
    rpc::FindLocationResponse resp;
    Status status =
        FromGrpc(CallMaster(&rpc::Master::Stub::FindLocation, req, &resp));
    if (!status.ok()) return status;
    bool found = false;
    for (const auto& chunk : resp.chunks()) {
      CachedChunk entry;
      entry.handle = chunk.handle();
      entry.version = chunk.version();
      entry.replicas.assign(chunk.replicas().begin(), chunk.replicas().end());
      if (chunk.index() == index) {
        *out = entry;
        found = true;
      }
      locations_.Put(path, chunk.index(), std::move(entry));
    }
    if (!found) *missing = true;
    return Status::Ok();
  }

  Status ReadChunk(const std::string& path, uint64_t index, uint64_t in_chunk,
                   uint64_t want, std::string* piece, bool* eof) {
    Status last = Status::Error(ErrorCode::kUnavailable, "no replicas");
    for (int attempt = 0; attempt < 2; ++attempt) {
      CachedChunk chunk;
      bool missing = false;
      Status status = Locate(path, index, &chunk, &missing);
      if (!status.ok()) return status;
      if (missing) {
        *eof = true;
        return Status::Ok();
      }
      size_t n = chunk.replicas.size();
      for (size_t k = 0; k < n; ++k) {
        const auto& replica = chunk.replicas[(replica_seed_ + k) % n];
        rpc::ReadRequest req;
        req.set_handle(chunk.handle);
        req.set_version(chunk.version);
        req.set_offset(in_chunk);
        req.set_length(want);
        rpc::ReadResponse resp;
        grpc::ClientContext ctx;
        SetDeadline(&ctx, config_.client_rpc_deadline);
        auto s = Chunkserver(replica.address())->Read(&ctx, req, &resp);
        if (!s.ok()) {
          last = FromGrpc(s);
          continue;
        }
        if (resp.code() == rpc::OK) {
          *piece = std::move(*resp.mutable_data());
          return Status::Ok();
        }
        if (resp.code() == rpc::OUT_OF_RANGE) {
          piece->clear();
          return Status::Ok();
        }
        last = FromResultCode(resp.code(), "read from " + replica.address());
      }
      locations_.Invalidate(path, index);
    }
    return Status::Error(ErrorCode::kUnavailable,
                         "no replica could serve chunk " +
                             std::to_string(index) + " of " + path + ": " +
                             last.message);
  }

  Status EnsureChunk(const std::string& path, uint64_t index) {
    FileInfo info;
    Status status = Open(path, &info);
    if (!status.ok()) return status;
    for (uint64_t next = info.chunk_count; next <= index; ++next) {
      rpc::AddChunkRequest req;
      req.set_path(path);
      req.set_index(next);
      rpc::AddChunkResponse resp;
      status = FromGrpc(CallMaster(&rpc::Master::Stub::AddChunk, req, &resp));
      if (!status.ok()) return status;
      if (resp.code() != rpc::OK)
        return FromResultCode(
            resp.code(), "add chunk " + std::to_string(next) + " to " + path);
    }
    return Status::Ok();
  }

  Status FindLease(const std::string& path, uint64_t index, CachedLease* out,
                   bool* from_cache) {
    if (auto cached = leases_.Get(path, index)) {
      *out = *cached;
      *from_cache = true;
      return Status::Ok();
    }
    *from_cache = false;
    rpc::FindLeaseHolderRequest req;
    req.set_path(path);
    req.set_index(index);
    rpc::FindLeaseHolderResponse resp;
    Status status = FromGrpc(CallMaster(&rpc::Master::Stub::FindLeaseHolder,
                                        req, &resp, LeaseWaitDeadline()));
    if (!status.ok()) return status;
    if (resp.code() != rpc::OK)
      return FromResultCode(resp.code(), "find lease holder for chunk " +
                                             std::to_string(index) + " of " +
                                             path);
    CachedLease lease;
    lease.handle = resp.handle();
    lease.version = resp.version();
    lease.primary = resp.primary();
    lease.secondaries.assign(resp.secondaries().begin(),
                             resp.secondaries().end());
    leases_.Put(path, index, lease);
    *out = lease;
    return Status::Ok();
  }

  Status PushData(const CachedLease& lease, uint64_t sequence,
                  std::string_view data) {
    std::vector<rpc::Replica> replicas;
    replicas.push_back(lease.primary);
    for (const auto& s : lease.secondaries) replicas.push_back(s);
    std::vector<rpc::Replica> chain =
        OrderPushChain({"", config_.rack}, replicas);
    rpc::PushDataResponse resp;
    grpc::ClientContext ctx;
    SetDeadline(&ctx, config_.client_rpc_deadline);
    auto writer = Chunkserver(chain[0].address())->PushData(&ctx, &resp);
    rpc::PushDataRequest header;
    header.mutable_header()->set_client_id(client_id_);
    header.mutable_header()->set_sequence(sequence);
    header.mutable_header()->set_total_length(data.size());
    for (size_t i = 1; i < chain.size(); ++i)
      *header.mutable_header()->add_forward_to() = chain[i];
    bool alive = writer->Write(header);
    size_t frame =
        config_.push_frame_size == 0 ? (256u << 10) : config_.push_frame_size;
    for (size_t pos = 0; alive && pos < data.size(); pos += frame) {
      rpc::PushDataRequest msg;
      msg.set_data(
          std::string(data.substr(pos, std::min(frame, data.size() - pos))));
      alive = writer->Write(msg);
    }
    writer->WritesDone();
    grpc::Status s = writer->Finish();
    if (!s.ok())
      return Status::Error(
          ErrorCode::kUnavailable,
          "push to " + chain[0].address() + ": " + s.error_message());
    if (resp.code() != rpc::OK) {
      return Status::Error(
          ErrorCode::kUnavailable,
          "push failed at " +
              (resp.failed_at().empty() ? chain[0].chunkserver_id()
                                        : resp.failed_at()) +
              ": " + rpc::ResultCode_Name(resp.code()));
    }
    return Status::Ok();
  }

  Millis LeaseWaitDeadline() const {
    return config_.lease_duration + 2 * config_.lease_clock_skew_margin +
           config_.client_rpc_deadline;
  }

  Millis Backoff(uint32_t attempt) const {
    return Millis(config_.retry_backoff_base.count()
                  << std::min<uint32_t>(attempt, 8));
  }

  Status TryWrite(const CachedLease& lease, uint64_t in_chunk,
                  std::string_view data) {
    Status last = Status::Error(ErrorCode::kFailed, "write retries exhausted");
    for (uint32_t inner = 0;
         inner < std::max<uint32_t>(config_.mutation_retry_inner, 1); ++inner) {
      if (inner > 0) SleepFor(Backoff(inner - 1));
      uint64_t sequence = sequence_.fetch_add(1);
      Status pushed = PushData(lease, sequence, data);
      if (!pushed.ok()) return pushed;
      rpc::WriteRequest req;
      req.set_handle(lease.handle);
      req.set_version(lease.version);
      req.set_offset(in_chunk);
      req.set_client_id(client_id_);
      req.set_sequence(sequence);
      rpc::WriteResponse resp;
      grpc::ClientContext ctx;
      SetDeadline(&ctx, config_.client_rpc_deadline);
      auto s = Chunkserver(lease.primary.address())->Write(&ctx, req, &resp);
      if (!s.ok())
        return Status::Error(ErrorCode::kUnavailable,
                             "write to primary " + lease.primary.address() +
                                 ": " + s.error_message());
      switch (resp.code()) {
        case rpc::OK:
          return Status::Ok();
        case rpc::DATA_MISSING:
        case rpc::FAILED:
          last = FromResultCode(resp.code(),
                                "write at primary " + lease.primary.address());
          continue;
        default:
          return FromResultCode(resp.code(),
                                "write at primary " + lease.primary.address());
      }
    }
    return last;
  }

  Status WriteChunk(const std::string& path, uint64_t index, uint64_t in_chunk,
                    std::string_view data) {
    Status last = Status::Error(ErrorCode::kFailed, "write retries exhausted");
    int outer = 0;
    bool ensured = false;
    while (outer <= static_cast<int>(config_.mutation_retry_outer)) {
      CachedLease lease;
      bool from_cache = false;
      Status status = FindLease(path, index, &lease, &from_cache);
      if (status.code == ErrorCode::kNotFound && !ensured) {
        status = EnsureChunk(path, index);
        if (status.code == ErrorCode::kNotFound ||
            status.code == ErrorCode::kInvalidArgument)
          return status;
        ensured = true;
        continue;
      }
      if (!status.ok()) {
        if (!IsRetryableLater(status)) return status;
        last = status;
        ++outer;
        SleepFor(config_.EffectiveOuterRetryDelay());
        continue;
      }
      Status attempt = TryWrite(lease, in_chunk, data);
      if (attempt.ok()) return Status::Ok();
      if (attempt.code == ErrorCode::kInvalidArgument) return attempt;
      last = attempt;
      leases_.Invalidate(path, index);
      if (from_cache) continue;
      ++outer;
      SleepFor(config_.EffectiveOuterRetryDelay());
    }
    return last;
  }

  Status TryAppend(const CachedLease& lease, std::string_view data,
                   uint64_t* in_chunk, bool* next_chunk) {
    Status last =
        Status::Error(ErrorCode::kFailed, "record append retries exhausted");
    for (uint32_t inner = 0;
         inner < std::max<uint32_t>(config_.mutation_retry_inner, 1); ++inner) {
      if (inner > 0) SleepFor(Backoff(inner - 1));
      uint64_t sequence = sequence_.fetch_add(1);
      Status pushed = PushData(lease, sequence, data);
      if (!pushed.ok()) return pushed;
      rpc::RecordAppendRequest req;
      req.set_handle(lease.handle);
      req.set_version(lease.version);
      req.set_client_id(client_id_);
      req.set_sequence(sequence);
      rpc::RecordAppendResponse resp;
      grpc::ClientContext ctx;
      SetDeadline(&ctx, config_.client_rpc_deadline);
      auto s =
          Chunkserver(lease.primary.address())->RecordAppend(&ctx, req, &resp);
      if (!s.ok())
        return Status::Error(ErrorCode::kUnavailable,
                             "record append to primary " +
                                 lease.primary.address() + ": " +
                                 s.error_message());
      switch (resp.code()) {
        case rpc::OK:
          *in_chunk = resp.offset();
          return Status::Ok();
        case rpc::RETRY_NEXT_CHUNK:
          *next_chunk = true;
          return Status::Error(ErrorCode::kFailed, "chunk full");
        case rpc::DATA_MISSING:
        case rpc::FAILED:
          last = FromResultCode(resp.code(), "record append at primary " +
                                                 lease.primary.address());
          continue;
        default:
          return FromResultCode(resp.code(), "record append at primary " +
                                                 lease.primary.address());
      }
    }
    return last;
  }

  Config config_;
  std::string client_id_;
  size_t replica_seed_;
  std::atomic<uint64_t> sequence_{1};
  std::unique_ptr<rpc::Master::Stub> master_;
  std::mutex stubs_mu_;
  std::map<std::string, std::unique_ptr<rpc::Chunkserver::Stub>> chunkservers_;
  LocationCache locations_;
  LeaseCache leases_;
  std::mutex info_mu_;
  bool info_loaded_ = false;
  uint64_t chunk_size_ = 0;
  uint64_t max_append_ = 0;
};

Client::Client(Config config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

Client::~Client() = default;

Status Client::Create(const std::string& path) { return impl_->Create(path); }
Status Client::Remove(const std::string& path) { return impl_->Remove(path); }
Status Client::Rename(const std::string& source, const std::string& target) {
  return impl_->Rename(source, target);
}
Status Client::Snapshot(const std::string& source, const std::string& target) {
  return impl_->Snapshot(source, target);
}
Status Client::List(const std::string& directory,
                    std::vector<DirEntry>* entries, bool include_hidden) {
  return impl_->List(directory, entries, include_hidden);
}
Status Client::Open(const std::string& path, FileInfo* info) {
  return impl_->Open(path, info);
}
Status Client::Length(const std::string& path, uint64_t* length) {
  return impl_->Length(path, length);
}
Status Client::Read(const std::string& path, uint64_t offset, uint64_t length,
                    std::string* data) {
  return impl_->Read(path, offset, length, data);
}
Status Client::Write(const std::string& path, uint64_t offset,
                     std::string_view data) {
  return impl_->Write(path, offset, data);
}
Status Client::RecordAppend(const std::string& path, std::string_view data,
                            uint64_t* offset) {
  return impl_->RecordAppend(path, data, offset);
}
uint64_t Client::ChunkSize() { return impl_->ChunkSize(); }
const std::string& Client::ClientId() const { return impl_->ClientId(); }

}  // namespace gfs
