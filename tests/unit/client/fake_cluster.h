#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/paths.h"
#include "gfs.grpc.pb.h"

namespace gfs::testing {

constexpr uint64_t kFakeChunkSize = 64 * 1024;
constexpr uint64_t kFakeMaxAppend = 16 * 1024;

struct FakeChunk {
  uint64_t version = 1;
  std::string data;
};

class FakeChunkserver final : public rpc::Chunkserver::Service {
 public:
  explicit FakeChunkserver(std::string id) : id_(std::move(id)) {}

  void start() {
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
    address_ = "127.0.0.1:" + std::to_string(port);
  }

  void stop() {
    if (server_) server_->Shutdown();
  }

  const std::string& id() const { return id_; }
  const std::string& address() const { return address_; }

  rpc::Replica replica() const {
    rpc::Replica r;
    r.set_chunkserver_id(id_);
    r.set_address(address_);
    return r;
  }

  void setPeers(std::vector<std::string> addresses) { peers_ = std::move(addresses); }

  void createChunk(uint64_t handle, uint64_t version) {
    std::lock_guard<std::mutex> lock(mu_);
    chunks_[handle] = FakeChunk{version, ""};
  }

  std::string chunkData(uint64_t handle) {
    std::lock_guard<std::mutex> lock(mu_);
    return chunks_[handle].data;
  }

  std::atomic<int> stale_reads{0};
  std::atomic<int> drop_data{0};
  std::atomic<int> pushes{0};

  grpc::Status PushData(grpc::ServerContext*, grpc::ServerReader<rpc::PushDataRequest>* reader, rpc::PushDataResponse* resp) override {
    rpc::PushDataRequest msg;
    if (!reader->Read(&msg) || !msg.has_header()) {
      resp->set_code(rpc::FAILED);
      return grpc::Status::OK;
    }
    rpc::PushHeader header = msg.header();
    std::string data;
    while (reader->Read(&msg)) data += msg.data();
    ++pushes;
    {
      std::lock_guard<std::mutex> lock(mu_);
      buffer_[{header.client_id(), header.sequence()}] = data;
    }
    if (header.forward_to_size() > 0) {
      auto stub = rpc::Chunkserver::NewStub(grpc::CreateChannel(header.forward_to(0).address(), grpc::InsecureChannelCredentials()));
      grpc::ClientContext ctx;
      rpc::PushDataResponse forwarded;
      auto writer = stub->PushData(&ctx, &forwarded);
      rpc::PushDataRequest first;
      *first.mutable_header() = header;
      first.mutable_header()->clear_forward_to();
      for (int i = 1; i < header.forward_to_size(); ++i) *first.mutable_header()->add_forward_to() = header.forward_to(i);
      writer->Write(first);
      rpc::PushDataRequest payload;
      payload.set_data(data);
      writer->Write(payload);
      writer->WritesDone();
      grpc::Status s = writer->Finish();
      if (!s.ok()) {
        resp->set_code(rpc::FAILED);
        resp->set_failed_at(header.forward_to(0).chunkserver_id());
        return grpc::Status::OK;
      }
      if (forwarded.code() != rpc::OK) {
        *resp = forwarded;
        return grpc::Status::OK;
      }
    }
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

  grpc::Status Read(grpc::ServerContext*, const rpc::ReadRequest* req, rpc::ReadResponse* resp) override {
    if (stale_reads > 0) {
      --stale_reads;
      resp->set_code(rpc::STALE_VERSION);
      return grpc::Status::OK;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = chunks_.find(req->handle());
    if (it == chunks_.end()) {
      resp->set_code(rpc::NO_SUCH_CHUNK);
      return grpc::Status::OK;
    }
    if (it->second.version < req->version()) {
      resp->set_code(rpc::STALE_VERSION);
      return grpc::Status::OK;
    }
    resp->set_code(rpc::OK);
    if (req->offset() < it->second.data.size()) resp->set_data(it->second.data.substr(req->offset(), req->length()));
    return grpc::Status::OK;
  }

  grpc::Status Write(grpc::ServerContext*, const rpc::WriteRequest* req, rpc::WriteResponse* resp) override {
    std::string data;
    if (!takeBuffer(req->client_id(), req->sequence(), &data)) {
      resp->set_code(rpc::DATA_MISSING);
      return grpc::Status::OK;
    }
    {
      std::lock_guard<std::mutex> lock(mu_);
      rpc::ResultCode code = applyLocked(req->handle(), req->version(), rpc::WRITE, req->offset(), data);
      if (code != rpc::OK) {
        resp->set_code(code);
        return grpc::Status::OK;
      }
    }
    resp->set_code(forward(req->handle(), req->version(), rpc::WRITE, req->offset(), req->client_id(), req->sequence()));
    return grpc::Status::OK;
  }

  grpc::Status RecordAppend(grpc::ServerContext*, const rpc::RecordAppendRequest* req, rpc::RecordAppendResponse* resp) override {
    std::string data;
    if (!takeBuffer(req->client_id(), req->sequence(), &data)) {
      resp->set_code(rpc::DATA_MISSING);
      return grpc::Status::OK;
    }
    uint64_t offset = 0;
    bool pad = false;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = chunks_.find(req->handle());
      if (it == chunks_.end()) {
        resp->set_code(rpc::NO_SUCH_CHUNK);
        return grpc::Status::OK;
      }
      if (it->second.version != req->version()) {
        resp->set_code(rpc::STALE_VERSION);
        return grpc::Status::OK;
      }
      offset = it->second.data.size();
      pad = offset + data.size() > kFakeChunkSize;
      applyLocked(req->handle(), req->version(), pad ? rpc::PAD : rpc::WRITE, offset, data);
    }
    rpc::ResultCode code = forward(req->handle(), req->version(), pad ? rpc::PAD : rpc::WRITE, offset, req->client_id(), req->sequence());
    if (code != rpc::OK) {
      resp->set_code(code);
      return grpc::Status::OK;
    }
    resp->set_code(pad ? rpc::RETRY_NEXT_CHUNK : rpc::OK);
    resp->set_offset(offset);
    return grpc::Status::OK;
  }

  grpc::Status GetChunkLength(grpc::ServerContext*, const rpc::GetChunkLengthRequest* req, rpc::GetChunkLengthResponse* resp) override {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = chunks_.find(req->handle());
    if (it == chunks_.end()) {
      resp->set_code(rpc::NO_SUCH_CHUNK);
      return grpc::Status::OK;
    }
    resp->set_code(rpc::OK);
    resp->set_length(it->second.data.size());
    return grpc::Status::OK;
  }

  grpc::Status ApplyMutation(grpc::ServerContext*, const rpc::ApplyMutationRequest* req, rpc::ApplyMutationResponse* resp) override {
    std::string data;
    if (req->kind() == rpc::WRITE && !takeBuffer(req->client_id(), req->sequence(), &data)) {
      resp->set_code(rpc::DATA_MISSING);
      return grpc::Status::OK;
    }
    std::lock_guard<std::mutex> lock(mu_);
    resp->set_code(applyLocked(req->handle(), req->version(), req->kind(), req->offset(), data));
    return grpc::Status::OK;
  }

  grpc::Status CreateChunk(grpc::ServerContext*, const rpc::CreateChunkRequest* req, rpc::CreateChunkResponse* resp) override {
    createChunk(req->handle(), req->version());
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

  grpc::Status GrantLease(grpc::ServerContext*, const rpc::GrantLeaseRequest*, rpc::GrantLeaseResponse* resp) override {
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

  grpc::Status RevokeLease(grpc::ServerContext*, const rpc::RevokeLeaseRequest*, rpc::RevokeLeaseResponse* resp) override {
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

  grpc::Status UpdateVersion(grpc::ServerContext*, const rpc::UpdateVersionRequest*, rpc::UpdateVersionResponse* resp) override {
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

 private:
  bool takeBuffer(const std::string& client, uint64_t sequence, std::string* data) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = buffer_.find({client, sequence});
    if (it == buffer_.end()) return false;
    *data = std::move(it->second);
    buffer_.erase(it);
    if (drop_data > 0) {
      --drop_data;
      return false;
    }
    return true;
  }

  rpc::ResultCode applyLocked(uint64_t handle, uint64_t version, rpc::MutationKind kind, uint64_t offset, const std::string& data) {
    auto it = chunks_.find(handle);
    if (it == chunks_.end()) return rpc::NO_SUCH_CHUNK;
    if (it->second.version != version) return rpc::STALE_VERSION;
    std::string& bytes = it->second.data;
    if (kind == rpc::PAD) {
      bytes.resize(kFakeChunkSize, '\0');
      return rpc::OK;
    }
    if (offset + data.size() > kFakeChunkSize) return rpc::OUT_OF_RANGE;
    if (bytes.size() < offset + data.size()) bytes.resize(offset + data.size(), '\0');
    bytes.replace(offset, data.size(), data);
    return rpc::OK;
  }

  rpc::ResultCode forward(uint64_t handle, uint64_t version, rpc::MutationKind kind, uint64_t offset, const std::string& client, uint64_t sequence) {
    for (const auto& peer : peers_) {
      auto stub = rpc::Chunkserver::NewStub(grpc::CreateChannel(peer, grpc::InsecureChannelCredentials()));
      grpc::ClientContext ctx;
      rpc::ApplyMutationRequest req;
      req.set_handle(handle);
      req.set_version(version);
      req.set_serial(++serial_);
      req.set_kind(kind);
      req.set_offset(offset);
      req.set_client_id(client);
      req.set_sequence(sequence);
      rpc::ApplyMutationResponse resp;
      grpc::Status s = stub->ApplyMutation(&ctx, req, &resp);
      if (!s.ok() || resp.code() != rpc::OK) return rpc::FAILED;
    }
    return rpc::OK;
  }

  std::string id_;
  std::string address_;
  std::unique_ptr<grpc::Server> server_;
  std::mutex mu_;
  std::map<uint64_t, FakeChunk> chunks_;
  std::map<std::pair<std::string, uint64_t>, std::string> buffer_;
  std::vector<std::string> peers_;
  std::atomic<uint64_t> serial_{0};
};

class FakeMaster final : public rpc::Master::Service {
 public:
  explicit FakeMaster(std::vector<FakeChunkserver*> servers) : servers_(std::move(servers)) {}

  void start() {
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
    address_ = "127.0.0.1:" + std::to_string(port);
  }

  void stop() {
    if (server_) server_->Shutdown();
  }

  const std::string& address() const { return address_; }

  grpc::Status GetClusterInfo(grpc::ServerContext*, const rpc::GetClusterInfoRequest*, rpc::GetClusterInfoResponse* resp) override {
    resp->set_chunk_size(kFakeChunkSize);
    resp->set_max_record_append_size(kFakeMaxAppend);
    return grpc::Status::OK;
  }

  grpc::Status Create(grpc::ServerContext*, const rpc::CreateRequest* req, rpc::CreateResponse*) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (!isValidPath(req->path()) || req->path() == "/") return {grpc::StatusCode::INVALID_ARGUMENT, "bad path"};
    if (files_.count(req->path())) return {grpc::StatusCode::ALREADY_EXISTS, "exists"};
    for (const auto& ancestor : ancestorsOf(req->path())) {
      if (files_.count(ancestor)) return {grpc::StatusCode::INVALID_ARGUMENT, "ancestor is a file"};
    }
    files_[req->path()] = {};
    return grpc::Status::OK;
  }

  grpc::Status Open(grpc::ServerContext*, const rpc::OpenRequest* req, rpc::OpenResponse* resp) override {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = files_.find(req->path());
    if (it == files_.end()) return {grpc::StatusCode::NOT_FOUND, "no such file"};
    resp->set_chunk_count(it->second.size());
    return grpc::Status::OK;
  }

  grpc::Status Delete(grpc::ServerContext*, const rpc::DeleteRequest* req, rpc::DeleteResponse*) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (!files_.erase(req->path())) return {grpc::StatusCode::NOT_FOUND, "no such file"};
    return grpc::Status::OK;
  }

  grpc::Status Rename(grpc::ServerContext*, const rpc::RenameRequest* req, rpc::RenameResponse*) override {
    std::lock_guard<std::mutex> lock(mu_);
    return moveLocked(req->source(), req->target(), true);
  }

  grpc::Status Snapshot(grpc::ServerContext*, const rpc::SnapshotRequest* req, rpc::SnapshotResponse*) override {
    std::lock_guard<std::mutex> lock(mu_);
    return moveLocked(req->source(), req->target(), false);
  }

  grpc::Status FindMatchingFiles(grpc::ServerContext*, const rpc::FindMatchingFilesRequest* req, rpc::FindMatchingFilesResponse* resp) override {
    std::lock_guard<std::mutex> lock(mu_);
    std::string prefix = childPrefix(req->directory());
    std::map<std::string, bool> seen;
    for (auto it = files_.lower_bound(prefix); it != files_.end() && it->first.compare(0, prefix.size(), prefix) == 0; ++it) {
      std::string rest = it->first.substr(prefix.size());
      size_t slash = rest.find('/');
      bool dir = slash != std::string::npos;
      std::string name = dir ? rest.substr(0, slash) : rest;
      if (!dir && !req->include_hidden() && isHiddenPath(it->first)) continue;
      seen[name] = seen[name] || dir;
    }
    for (const auto& [name, dir] : seen) {
      auto* e = resp->add_entries();
      e->set_name(name);
      e->set_is_directory(dir);
    }
    return grpc::Status::OK;
  }

  grpc::Status FindLocation(grpc::ServerContext*, const rpc::FindLocationRequest* req, rpc::FindLocationResponse* resp) override {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = files_.find(req->path());
    if (it == files_.end()) return {grpc::StatusCode::NOT_FOUND, "no such file"};
    for (uint64_t i = req->first_index(); i < it->second.size() && i < req->first_index() + req->count(); ++i) {
      auto* chunk = resp->add_chunks();
      chunk->set_index(i);
      chunk->set_handle(it->second[i]);
      chunk->set_version(versions_[it->second[i]]);
      for (auto* s : servers_) *chunk->add_replicas() = s->replica();
    }
    return grpc::Status::OK;
  }

  grpc::Status FindLeaseHolder(grpc::ServerContext*, const rpc::FindLeaseHolderRequest* req, rpc::FindLeaseHolderResponse* resp) override {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = files_.find(req->path());
    if (it == files_.end()) return {grpc::StatusCode::NOT_FOUND, "no such file"};
    if (req->index() >= it->second.size()) return {grpc::StatusCode::NOT_FOUND, "no such chunk index"};
    ++lease_requests;
    resp->set_code(rpc::OK);
    resp->set_handle(it->second[req->index()]);
    resp->set_version(versions_[it->second[req->index()]]);
    *resp->mutable_primary() = servers_[0]->replica();
    for (size_t i = 1; i < servers_.size(); ++i) *resp->add_secondaries() = servers_[i]->replica();
    return grpc::Status::OK;
  }

  grpc::Status AddChunk(grpc::ServerContext*, const rpc::AddChunkRequest* req, rpc::AddChunkResponse* resp) override {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = files_.find(req->path());
    if (it == files_.end()) return {grpc::StatusCode::NOT_FOUND, "no such file"};
    if (req->index() > it->second.size()) return {grpc::StatusCode::INVALID_ARGUMENT, "chunk index leaves a hole"};
    if (req->index() == it->second.size()) {
      uint64_t handle = next_handle_++;
      versions_[handle] = 1;
      for (auto* s : servers_) s->createChunk(handle, 1);
      it->second.push_back(handle);
    }
    resp->set_code(rpc::OK);
    auto* chunk = resp->mutable_chunk();
    chunk->set_index(req->index());
    chunk->set_handle(it->second[req->index()]);
    chunk->set_version(versions_[it->second[req->index()]]);
    for (auto* s : servers_) *chunk->add_replicas() = s->replica();
    return grpc::Status::OK;
  }

  grpc::Status HeartBeat(grpc::ServerContext*, const rpc::HeartBeatRequest*, rpc::HeartBeatResponse*) override {
    return grpc::Status::OK;
  }

  std::atomic<int> lease_requests{0};

 private:
  grpc::Status moveLocked(const std::string& source, const std::string& target, bool erase_source) {
    if (!isValidPath(source) || !isValidPath(target)) return {grpc::StatusCode::INVALID_ARGUMENT, "bad path"};
    if (existsLocked(target)) return {grpc::StatusCode::ALREADY_EXISTS, "target exists"};
    std::vector<std::pair<std::string, std::vector<uint64_t>>> moved;
    if (files_.count(source)) {
      moved.emplace_back(target, files_[source]);
    } else {
      std::string prefix = childPrefix(source);
      for (auto it = files_.lower_bound(prefix); it != files_.end() && it->first.compare(0, prefix.size(), prefix) == 0; ++it) {
        moved.emplace_back(childPrefix(target) + it->first.substr(prefix.size()), it->second);
      }
      if (moved.empty()) return {grpc::StatusCode::NOT_FOUND, "no such file or directory"};
    }
    if (erase_source) {
      files_.erase(source);
      std::string prefix = childPrefix(source);
      for (auto it = files_.lower_bound(prefix); it != files_.end() && it->first.compare(0, prefix.size(), prefix) == 0;) it = files_.erase(it);
    }
    for (auto& [path, handles] : moved) files_[path] = handles;
    return grpc::Status::OK;
  }

  bool existsLocked(const std::string& path) {
    if (files_.count(path)) return true;
    std::string prefix = childPrefix(path);
    auto it = files_.lower_bound(prefix);
    return it != files_.end() && it->first.compare(0, prefix.size(), prefix) == 0;
  }

  std::vector<FakeChunkserver*> servers_;
  std::string address_;
  std::unique_ptr<grpc::Server> server_;
  std::mutex mu_;
  std::map<std::string, std::vector<uint64_t>> files_;
  std::map<uint64_t, uint64_t> versions_;
  uint64_t next_handle_ = 1;
};

class FakeCluster {
 public:
  explicit FakeCluster(int chunkservers = 3) {
    for (int i = 0; i < chunkservers; ++i) {
      servers.push_back(std::make_unique<FakeChunkserver>("cs" + std::to_string(i)));
      servers.back()->start();
    }
    for (size_t i = 0; i < servers.size(); ++i) {
      std::vector<std::string> peers;
      for (size_t j = 0; j < servers.size(); ++j) {
        if (j != i) peers.push_back(servers[j]->address());
      }
      servers[i]->setPeers(peers);
    }
    std::vector<FakeChunkserver*> raw;
    for (auto& s : servers) raw.push_back(s.get());
    master = std::make_unique<FakeMaster>(raw);
    master->start();
  }

  ~FakeCluster() {
    master->stop();
    for (auto& s : servers) s->stop();
  }

  Config clientConfig() const {
    Config config;
    config.master_address = master->address();
    config.client_rpc_deadline = Millis(2000);
    config.lease_duration = Millis(1000);
    config.lease_clock_skew_margin = Millis(100);
    config.retry_backoff_base = Millis(10);
    config.outer_retry_delay = Millis(30);
    config.mutation_retry_inner = 3;
    config.mutation_retry_outer = 2;
    config.client_location_cache_ttl = Millis(5000);
    config.push_frame_size = 8 * 1024;
    return config;
  }

  std::vector<std::unique_ptr<FakeChunkserver>> servers;
  std::unique_ptr<FakeMaster> master;
};

}
