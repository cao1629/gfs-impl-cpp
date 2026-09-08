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

Status fromGrpc(const grpc::Status& status) {
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

Status fromResultCode(rpc::ResultCode code, const std::string& where) {
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

void setDeadline(grpc::ClientContext* ctx, Millis timeout) {
  ctx->set_deadline(std::chrono::system_clock::now() + timeout);
}

void sleepFor(Millis duration) {
  if (duration.count() > 0) std::this_thread::sleep_for(duration);
}

bool isRetryableLater(const Status& status) {
  return status.code == ErrorCode::kUnavailable || status.code == ErrorCode::kStale || status.code == ErrorCode::kFailed;
}

}

class Client::Impl {
 public:
  explicit Impl(Config config)
      : config_(std::move(config)),
        client_id_(randomHexId()),
        replica_seed_(std::hash<std::string>{}(client_id_)),
        master_(rpc::Master::NewStub(grpc::CreateChannel(config_.master_address, grpc::InsecureChannelCredentials()))),
        locations_(config_.client_location_cache_ttl),
        leases_(config_.client_location_cache_ttl) {}

  Status create(const std::string& path) {
    rpc::CreateRequest req;
    req.set_path(path);
    rpc::CreateResponse resp;
    return fromGrpc(callMaster(&rpc::Master::Stub::Create, req, &resp));
  }

  Status remove(const std::string& path) {
    rpc::DeleteRequest req;
    req.set_path(path);
    rpc::DeleteResponse resp;
    Status status = fromGrpc(callMaster(&rpc::Master::Stub::Delete, req, &resp));
    forgetFile(path);
    return status;
  }

  Status rename(const std::string& source, const std::string& target) {
    rpc::RenameRequest req;
    req.set_source(source);
    req.set_target(target);
    rpc::RenameResponse resp;
    Status status = fromGrpc(callMaster(&rpc::Master::Stub::Rename, req, &resp));
    forgetFile(source);
    forgetFile(target);
    return status;
  }

  Status snapshot(const std::string& source, const std::string& target) {
    rpc::SnapshotRequest req;
    req.set_source(source);
    req.set_target(target);
    rpc::SnapshotResponse resp;
    Status status = fromGrpc(callMaster(&rpc::Master::Stub::Snapshot, req, &resp));
    forgetFile(source);
    return status;
  }

  Status list(const std::string& directory, std::vector<DirEntry>* entries, bool include_hidden) {
    rpc::FindMatchingFilesRequest req;
    req.set_directory(directory);
    req.set_include_hidden(include_hidden);
    rpc::FindMatchingFilesResponse resp;
    Status status = fromGrpc(callMaster(&rpc::Master::Stub::FindMatchingFiles, req, &resp));
    if (!status.ok()) return status;
    entries->clear();
    for (const auto& e : resp.entries()) entries->push_back({e.name(), e.is_directory()});
    return Status::Ok();
  }

  Status open(const std::string& path, FileInfo* info) {
    rpc::OpenRequest req;
    req.set_path(path);
    rpc::OpenResponse resp;
    Status status = fromGrpc(callMaster(&rpc::Master::Stub::Open, req, &resp));
    if (!status.ok()) return status;
    if (info) info->chunk_count = resp.chunk_count();
    return Status::Ok();
  }

  Status length(const std::string& path, uint64_t* length) {
    Status status = ensureClusterInfo();
    if (!status.ok()) return status;
    FileInfo info;
    status = open(path, &info);
    if (!status.ok()) return status;
    if (info.chunk_count == 0) {
      *length = 0;
      return Status::Ok();
    }
    uint64_t last = info.chunk_count - 1;
    for (int attempt = 0; attempt < 2; ++attempt) {
      CachedChunk chunk;
      bool missing = false;
      status = locate(path, last, &chunk, &missing);
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
        setDeadline(&ctx, config_.client_rpc_deadline);
        auto s = chunkserver(replica.address())->GetChunkLength(&ctx, req, &resp);
        if (!s.ok() || resp.code() != rpc::OK) continue;
        *length = last * chunk_size_ + resp.length();
        return Status::Ok();
      }
      locations_.invalidate(path, last);
    }
    return Status::Error(ErrorCode::kUnavailable, "no replica could report the length of the last chunk");
  }

  Status read(const std::string& path, uint64_t offset, uint64_t length, std::string* data) {
    data->clear();
    if (length == 0) return Status::Ok();
    Status status = ensureClusterInfo();
    if (!status.ok()) return status;
    uint64_t pos = offset;
    uint64_t end = offset + length;
    while (pos < end) {
      uint64_t index = pos / chunk_size_;
      uint64_t in_chunk = pos % chunk_size_;
      uint64_t want = std::min(end - pos, chunk_size_ - in_chunk);
      std::string piece;
      bool eof = false;
      status = readChunk(path, index, in_chunk, want, &piece, &eof);
      if (!status.ok()) return status;
      if (eof) break;
      data->append(piece);
      pos += piece.size();
      if (piece.size() < want) break;
    }
    return Status::Ok();
  }

  Status write(const std::string& path, uint64_t offset, std::string_view data) {
    if (data.empty()) return Status::Ok();
    Status status = ensureClusterInfo();
    if (!status.ok()) return status;
    uint64_t pos = 0;
    while (pos < data.size()) {
      uint64_t absolute = offset + pos;
      uint64_t index = absolute / chunk_size_;
      uint64_t in_chunk = absolute % chunk_size_;
      uint64_t len = std::min<uint64_t>(data.size() - pos, chunk_size_ - in_chunk);
      status = writeChunk(path, index, in_chunk, data.substr(pos, len));
      if (!status.ok()) return status;
      pos += len;
    }
    return Status::Ok();
  }

  Status recordAppend(const std::string& path, std::string_view data, uint64_t* offset) {
    Status status = ensureClusterInfo();
    if (!status.ok()) return status;
    if (data.size() > max_append_) {
      return Status::Error(ErrorCode::kInvalidArgument,
                           "record of " + std::to_string(data.size()) + " bytes exceeds the append limit of " + std::to_string(max_append_));
    }
    FileInfo info;
    status = open(path, &info);
    if (!status.ok()) return status;
    uint64_t index = info.chunk_count == 0 ? 0 : info.chunk_count - 1;
    if (info.chunk_count == 0) {
      status = ensureChunk(path, 0);
      if (!status.ok()) return status;
    }
    Status last = Status::Error(ErrorCode::kFailed, "record append retries exhausted");
    int outer = 0;
    bool ensured = false;
    while (outer <= static_cast<int>(config_.mutation_retry_outer)) {
      CachedLease lease;
      bool from_cache = false;
      status = findLease(path, index, &lease, &from_cache);
      if (status.code == ErrorCode::kNotFound && !ensured) {
        status = ensureChunk(path, index);
        if (status.code == ErrorCode::kNotFound || status.code == ErrorCode::kInvalidArgument) return status;
        ensured = true;
        continue;
      }
      if (!status.ok()) {
        if (!isRetryableLater(status)) return status;
        last = status;
        ++outer;
        sleepFor(config_.effectiveOuterRetryDelay());
        continue;
      }
      bool next_chunk = false;
      uint64_t in_chunk = 0;
      Status attempt = tryAppend(lease, data, &in_chunk, &next_chunk);
      if (attempt.ok()) {
        if (offset) *offset = index * chunk_size_ + in_chunk;
        return Status::Ok();
      }
      if (next_chunk) {
        status = ensureChunk(path, index + 1);
        if (!status.ok() && !isRetryableLater(status)) return status;
        if (status.ok()) {
          ++index;
          ensured = true;
          outer = 0;
        }
        continue;
      }
      if (attempt.code == ErrorCode::kInvalidArgument) return attempt;
      last = attempt;
      leases_.invalidate(path, index);
      if (from_cache) continue;
      ++outer;
      sleepFor(config_.effectiveOuterRetryDelay());
    }
    return last;
  }

  uint64_t chunkSize() {
    ensureClusterInfo();
    return chunk_size_;
  }

  const std::string& clientId() const { return client_id_; }

 private:
  template <typename Req, typename Resp>
  grpc::Status callMaster(grpc::Status (rpc::Master::Stub::*method)(grpc::ClientContext*, const Req&, Resp*), const Req& req,
                          Resp* resp, Millis timeout) {
    grpc::ClientContext ctx;
    setDeadline(&ctx, timeout);
    return (master_.get()->*method)(&ctx, req, resp);
  }

  template <typename Req, typename Resp>
  grpc::Status callMaster(grpc::Status (rpc::Master::Stub::*method)(grpc::ClientContext*, const Req&, Resp*), const Req& req,
                          Resp* resp) {
    return callMaster(method, req, resp, config_.client_rpc_deadline);
  }

  rpc::Chunkserver::Stub* chunkserver(const std::string& address) {
    std::lock_guard<std::mutex> lock(stubs_mu_);
    auto& stub = chunkservers_[address];
    if (!stub) {
      grpc::ChannelArguments args;
      args.SetMaxReceiveMessageSize(-1);
      args.SetMaxSendMessageSize(-1);
      stub = rpc::Chunkserver::NewStub(grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args));
    }
    return stub.get();
  }

  Status ensureClusterInfo() {
    std::lock_guard<std::mutex> lock(info_mu_);
    if (info_loaded_) return Status::Ok();
    rpc::GetClusterInfoRequest req;
    rpc::GetClusterInfoResponse resp;
    Status status = fromGrpc(callMaster(&rpc::Master::Stub::GetClusterInfo, req, &resp));
    if (!status.ok()) return status;
    if (resp.chunk_size() == 0) return Status::Error(ErrorCode::kFailed, "master reported a chunk size of zero");
    chunk_size_ = resp.chunk_size();
    max_append_ = resp.max_record_append_size() == 0 ? chunk_size_ / 4 : resp.max_record_append_size();
    info_loaded_ = true;
    return Status::Ok();
  }

  void forgetFile(const std::string& path) {
    locations_.invalidateFile(path);
    leases_.invalidateFile(path);
  }

  Status locate(const std::string& path, uint64_t index, CachedChunk* out, bool* missing) {
    *missing = false;
    if (auto cached = locations_.get(path, index)) {
      *out = *cached;
      return Status::Ok();
    }
    rpc::FindLocationRequest req;
    req.set_path(path);
    req.set_first_index(index);
    req.set_count(kLocationBatch);
    rpc::FindLocationResponse resp;
    Status status = fromGrpc(callMaster(&rpc::Master::Stub::FindLocation, req, &resp));
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
      locations_.put(path, chunk.index(), std::move(entry));
    }
    if (!found) *missing = true;
    return Status::Ok();
  }

  Status readChunk(const std::string& path, uint64_t index, uint64_t in_chunk, uint64_t want, std::string* piece, bool* eof) {
    Status last = Status::Error(ErrorCode::kUnavailable, "no replicas");
    for (int attempt = 0; attempt < 2; ++attempt) {
      CachedChunk chunk;
      bool missing = false;
      Status status = locate(path, index, &chunk, &missing);
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
        setDeadline(&ctx, config_.client_rpc_deadline);
        auto s = chunkserver(replica.address())->Read(&ctx, req, &resp);
        if (!s.ok()) {
          last = fromGrpc(s);
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
        last = fromResultCode(resp.code(), "read from " + replica.address());
      }
      locations_.invalidate(path, index);
    }
    return Status::Error(ErrorCode::kUnavailable,
                         "no replica could serve chunk " + std::to_string(index) + " of " + path + ": " + last.message);
  }

  Status ensureChunk(const std::string& path, uint64_t index) {
    FileInfo info;
    Status status = open(path, &info);
    if (!status.ok()) return status;
    for (uint64_t next = info.chunk_count; next <= index; ++next) {
      rpc::AddChunkRequest req;
      req.set_path(path);
      req.set_index(next);
      rpc::AddChunkResponse resp;
      status = fromGrpc(callMaster(&rpc::Master::Stub::AddChunk, req, &resp));
      if (!status.ok()) return status;
      if (resp.code() != rpc::OK) return fromResultCode(resp.code(), "add chunk " + std::to_string(next) + " to " + path);
    }
    return Status::Ok();
  }

  Status findLease(const std::string& path, uint64_t index, CachedLease* out, bool* from_cache) {
    if (auto cached = leases_.get(path, index)) {
      *out = *cached;
      *from_cache = true;
      return Status::Ok();
    }
    *from_cache = false;
    rpc::FindLeaseHolderRequest req;
    req.set_path(path);
    req.set_index(index);
    rpc::FindLeaseHolderResponse resp;
    Millis timeout = config_.lease_duration + 2 * config_.lease_clock_skew_margin + config_.client_rpc_deadline;
    Status status = fromGrpc(callMaster(&rpc::Master::Stub::FindLeaseHolder, req, &resp, timeout));
    if (!status.ok()) return status;
    if (resp.code() != rpc::OK) return fromResultCode(resp.code(), "find lease holder for chunk " + std::to_string(index) + " of " + path);
    CachedLease lease;
    lease.handle = resp.handle();
    lease.version = resp.version();
    lease.primary = resp.primary();
    lease.secondaries.assign(resp.secondaries().begin(), resp.secondaries().end());
    leases_.put(path, index, lease);
    *out = lease;
    return Status::Ok();
  }

  Status pushData(const CachedLease& lease, uint64_t sequence, std::string_view data) {
    std::vector<rpc::Replica> replicas;
    replicas.push_back(lease.primary);
    for (const auto& s : lease.secondaries) replicas.push_back(s);
    std::vector<rpc::Replica> chain = orderPushChain({"", config_.rack}, replicas);
    rpc::PushDataResponse resp;
    grpc::ClientContext ctx;
    setDeadline(&ctx, config_.client_rpc_deadline);
    auto writer = chunkserver(chain[0].address())->PushData(&ctx, &resp);
    rpc::PushDataRequest header;
    header.mutable_header()->set_client_id(client_id_);
    header.mutable_header()->set_sequence(sequence);
    header.mutable_header()->set_total_length(data.size());
    for (size_t i = 1; i < chain.size(); ++i) *header.mutable_header()->add_forward_to() = chain[i];
    bool alive = writer->Write(header);
    size_t frame = config_.push_frame_size == 0 ? (256u << 10) : config_.push_frame_size;
    for (size_t pos = 0; alive && pos < data.size(); pos += frame) {
      rpc::PushDataRequest msg;
      msg.set_data(std::string(data.substr(pos, std::min(frame, data.size() - pos))));
      alive = writer->Write(msg);
    }
    writer->WritesDone();
    grpc::Status s = writer->Finish();
    if (!s.ok()) return Status::Error(ErrorCode::kUnavailable, "push to " + chain[0].address() + ": " + s.error_message());
    if (resp.code() != rpc::OK) {
      return Status::Error(ErrorCode::kUnavailable, "push failed at " + (resp.failed_at().empty() ? chain[0].chunkserver_id() : resp.failed_at()) +
                                                        ": " + rpc::ResultCode_Name(resp.code()));
    }
    return Status::Ok();
  }

  Millis backoff(uint32_t attempt) const {
    return Millis(config_.retry_backoff_base.count() << std::min<uint32_t>(attempt, 8));
  }

  Status tryWrite(const CachedLease& lease, uint64_t in_chunk, std::string_view data) {
    Status last = Status::Error(ErrorCode::kFailed, "write retries exhausted");
    for (uint32_t inner = 0; inner < std::max<uint32_t>(config_.mutation_retry_inner, 1); ++inner) {
      if (inner > 0) sleepFor(backoff(inner - 1));
      uint64_t sequence = sequence_.fetch_add(1);
      Status pushed = pushData(lease, sequence, data);
      if (!pushed.ok()) return pushed;
      rpc::WriteRequest req;
      req.set_handle(lease.handle);
      req.set_version(lease.version);
      req.set_offset(in_chunk);
      req.set_client_id(client_id_);
      req.set_sequence(sequence);
      rpc::WriteResponse resp;
      grpc::ClientContext ctx;
      setDeadline(&ctx, config_.client_rpc_deadline);
      auto s = chunkserver(lease.primary.address())->Write(&ctx, req, &resp);
      if (!s.ok()) return Status::Error(ErrorCode::kUnavailable, "write to primary " + lease.primary.address() + ": " + s.error_message());
      switch (resp.code()) {
        case rpc::OK:
          return Status::Ok();
        case rpc::DATA_MISSING:
        case rpc::FAILED:
          last = fromResultCode(resp.code(), "write at primary " + lease.primary.address());
          continue;
        default:
          return fromResultCode(resp.code(), "write at primary " + lease.primary.address());
      }
    }
    return last;
  }

  Status writeChunk(const std::string& path, uint64_t index, uint64_t in_chunk, std::string_view data) {
    Status last = Status::Error(ErrorCode::kFailed, "write retries exhausted");
    int outer = 0;
    bool ensured = false;
    while (outer <= static_cast<int>(config_.mutation_retry_outer)) {
      CachedLease lease;
      bool from_cache = false;
      Status status = findLease(path, index, &lease, &from_cache);
      if (status.code == ErrorCode::kNotFound && !ensured) {
        status = ensureChunk(path, index);
        if (status.code == ErrorCode::kNotFound || status.code == ErrorCode::kInvalidArgument) return status;
        ensured = true;
        continue;
      }
      if (!status.ok()) {
        if (!isRetryableLater(status)) return status;
        last = status;
        ++outer;
        sleepFor(config_.effectiveOuterRetryDelay());
        continue;
      }
      Status attempt = tryWrite(lease, in_chunk, data);
      if (attempt.ok()) return Status::Ok();
      if (attempt.code == ErrorCode::kInvalidArgument) return attempt;
      last = attempt;
      leases_.invalidate(path, index);
      if (from_cache) continue;
      ++outer;
      sleepFor(config_.effectiveOuterRetryDelay());
    }
    return last;
  }

  Status tryAppend(const CachedLease& lease, std::string_view data, uint64_t* in_chunk, bool* next_chunk) {
    Status last = Status::Error(ErrorCode::kFailed, "record append retries exhausted");
    for (uint32_t inner = 0; inner < std::max<uint32_t>(config_.mutation_retry_inner, 1); ++inner) {
      if (inner > 0) sleepFor(backoff(inner - 1));
      uint64_t sequence = sequence_.fetch_add(1);
      Status pushed = pushData(lease, sequence, data);
      if (!pushed.ok()) return pushed;
      rpc::RecordAppendRequest req;
      req.set_handle(lease.handle);
      req.set_version(lease.version);
      req.set_client_id(client_id_);
      req.set_sequence(sequence);
      rpc::RecordAppendResponse resp;
      grpc::ClientContext ctx;
      setDeadline(&ctx, config_.client_rpc_deadline);
      auto s = chunkserver(lease.primary.address())->RecordAppend(&ctx, req, &resp);
      if (!s.ok()) return Status::Error(ErrorCode::kUnavailable, "record append to primary " + lease.primary.address() + ": " + s.error_message());
      switch (resp.code()) {
        case rpc::OK:
          *in_chunk = resp.offset();
          return Status::Ok();
        case rpc::RETRY_NEXT_CHUNK:
          *next_chunk = true;
          return Status::Error(ErrorCode::kFailed, "chunk full");
        case rpc::DATA_MISSING:
        case rpc::FAILED:
          last = fromResultCode(resp.code(), "record append at primary " + lease.primary.address());
          continue;
        default:
          return fromResultCode(resp.code(), "record append at primary " + lease.primary.address());
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

Client::Client(Config config) : impl_(std::make_unique<Impl>(std::move(config))) {}

Client::~Client() = default;

Status Client::create(const std::string& path) { return impl_->create(path); }
Status Client::remove(const std::string& path) { return impl_->remove(path); }
Status Client::rename(const std::string& source, const std::string& target) { return impl_->rename(source, target); }
Status Client::snapshot(const std::string& source, const std::string& target) { return impl_->snapshot(source, target); }
Status Client::list(const std::string& directory, std::vector<DirEntry>* entries, bool include_hidden) {
  return impl_->list(directory, entries, include_hidden);
}
Status Client::open(const std::string& path, FileInfo* info) { return impl_->open(path, info); }
Status Client::length(const std::string& path, uint64_t* length) { return impl_->length(path, length); }
Status Client::read(const std::string& path, uint64_t offset, uint64_t length, std::string* data) {
  return impl_->read(path, offset, length, data);
}
Status Client::write(const std::string& path, uint64_t offset, std::string_view data) { return impl_->write(path, offset, data); }
Status Client::recordAppend(const std::string& path, std::string_view data, uint64_t* offset) {
  return impl_->recordAppend(path, data, offset);
}
uint64_t Client::chunkSize() { return impl_->chunkSize(); }
const std::string& Client::clientId() const { return impl_->clientId(); }

}
