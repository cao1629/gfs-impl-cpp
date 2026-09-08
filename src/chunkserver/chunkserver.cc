#include "chunkserver/chunkserver.h"

#include <chrono>

#include "common/ids.h"
#include "common/logging.h"

namespace gfs {

namespace {

std::chrono::system_clock::time_point deadlineAfter(Millis d) {
  return std::chrono::system_clock::now() + d;
}

}

Chunkserver::Chunkserver(Config config, std::string id, ChunkStore* store)
    : config_(std::move(config)),
      id_(std::move(id)),
      store_(store),
      buffer_(config_.data_buffer_capacity),
      leases_(config_.lease_clock_skew_margin),
      advertise_(config_.effectiveAdvertise()) {
  master_ = rpc::Master::NewStub(grpc::CreateChannel(config_.master_address, grpc::InsecureChannelCredentials()));
}

Chunkserver::~Chunkserver() {
  stop();
}

void Chunkserver::setAdvertiseAddress(std::string address) {
  std::lock_guard<std::mutex> lock(advertise_mutex_);
  advertise_ = std::move(address);
}

rpc::Chunkserver::Stub* Chunkserver::stubFor(const std::string& address) {
  std::lock_guard<std::mutex> lock(stubs_mutex_);
  auto& stub = stubs_[address];
  if (!stub) {
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(static_cast<int>(std::min<uint64_t>(config_.chunk_size + (1 << 20), INT32_MAX)));
    args.SetMaxSendMessageSize(static_cast<int>(std::min<uint64_t>(config_.chunk_size + (1 << 20), INT32_MAX)));
    stub = rpc::Chunkserver::NewStub(grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args));
  }
  return stub.get();
}

rpc::PushDataResponse Chunkserver::pushData(grpc::ServerReader<rpc::PushDataRequest>* reader) {
  rpc::PushDataResponse resp;
  rpc::PushDataRequest msg;
  if (!reader->Read(&msg) || !msg.has_header()) {
    resp.set_code(rpc::FAILED);
    resp.set_failed_at(id_);
    return resp;
  }
  rpc::PushHeader header = msg.header();
  std::string data;
  data.reserve(static_cast<size_t>(header.total_length()));

  std::unique_ptr<grpc::ClientContext> ctx;
  std::unique_ptr<grpc::ClientWriter<rpc::PushDataRequest>> writer;
  rpc::PushDataResponse downstream;
  rpc::Replica next;
  if (header.forward_to_size() > 0) {
    next = header.forward_to(0);
    ctx = std::make_unique<grpc::ClientContext>();
    ctx->set_deadline(deadlineAfter(config_.client_rpc_deadline));
    writer = stubFor(next.address())->PushData(ctx.get(), &downstream);
    rpc::PushDataRequest forwarded;
    *forwarded.mutable_header() = header;
    forwarded.mutable_header()->clear_forward_to();
    for (int i = 1; i < header.forward_to_size(); ++i) *forwarded.mutable_header()->add_forward_to() = header.forward_to(i);
    if (!writer->Write(forwarded)) {
      writer->Finish();
      resp.set_code(rpc::FAILED);
      resp.set_failed_at(next.chunkserver_id());
      return resp;
    }
  }

  bool forward_broken = false;
  while (reader->Read(&msg)) {
    if (!msg.has_data()) continue;
    data.append(msg.data());
    if (writer && !forward_broken && !writer->Write(msg)) forward_broken = true;
  }

  if (writer) {
    writer->WritesDone();
    grpc::Status status = writer->Finish();
    if (forward_broken || !status.ok()) {
      GFS_LOG_WARN << "push forward to " << next.address() << " failed: " << status.error_message();
      resp.set_code(rpc::FAILED);
      resp.set_failed_at(next.chunkserver_id());
      return resp;
    }
    if (downstream.code() != rpc::OK) {
      resp.set_code(downstream.code());
      resp.set_failed_at(downstream.failed_at());
      return resp;
    }
  }

  if (header.total_length() != 0 && data.size() != header.total_length()) {
    resp.set_code(rpc::FAILED);
    resp.set_failed_at(id_);
    return resp;
  }
  buffer_.put({header.client_id(), header.sequence()}, std::move(data));
  resp.set_code(rpc::OK);
  return resp;
}

rpc::ReadResponse Chunkserver::read(const rpc::ReadRequest& req) {
  rpc::ReadResponse resp;
  auto version = store_->version(req.handle());
  if (!version) {
    resp.set_code(rpc::NO_SUCH_CHUNK);
    return resp;
  }
  if (*version < req.version()) {
    resp.set_code(rpc::STALE_VERSION);
    return resp;
  }
  resp.set_code(store_->read(req.handle(), req.offset(), req.length(), resp.mutable_data()));
  return resp;
}

rpc::ResultCode Chunkserver::prepareMutation(uint64_t handle, uint64_t version, Prepared* out) {
  out->lock = store_->lockForMutation(handle);
  if (!out->lock) return rpc::NO_SUCH_CHUNK;
  auto current = store_->version(handle);
  if (!current) return rpc::NO_SUCH_CHUNK;
  if (*current != version) return rpc::STALE_VERSION;
  switch (leases_.check(handle, &out->lease)) {
    case LeaseCheck::kNotHeld: return rpc::NOT_PRIMARY;
    case LeaseCheck::kExpired: return rpc::LEASE_EXPIRED;
    case LeaseCheck::kPrimary: break;
  }
  return rpc::OK;
}

std::optional<std::string> Chunkserver::forwardMutation(const rpc::ApplyMutationRequest& req, const std::vector<rpc::Replica>& secondaries) {
  std::vector<std::thread> threads;
  std::vector<bool> ok(secondaries.size(), false);
  for (size_t i = 0; i < secondaries.size(); ++i) {
    threads.emplace_back([&, i] {
      grpc::ClientContext ctx;
      ctx.set_deadline(deadlineAfter(config_.rpc_deadline));
      rpc::ApplyMutationResponse resp;
      grpc::Status status = stubFor(secondaries[i].address())->ApplyMutation(&ctx, req, &resp);
      if (!status.ok()) {
        GFS_LOG_WARN << "apply mutation on " << secondaries[i].address() << " failed: " << status.error_message();
      } else if (resp.code() != rpc::OK) {
        GFS_LOG_WARN << "apply mutation on " << secondaries[i].address() << " rejected: " << rpc::ResultCode_Name(resp.code());
      } else {
        ok[i] = true;
      }
    });
  }
  for (auto& t : threads) t.join();
  for (size_t i = 0; i < secondaries.size(); ++i) {
    if (!ok[i]) return secondaries[i].chunkserver_id();
  }
  return std::nullopt;
}

rpc::WriteResponse Chunkserver::write(const rpc::WriteRequest& req) {
  rpc::WriteResponse resp;
  Prepared prepared;
  rpc::ResultCode code = prepareMutation(req.handle(), req.version(), &prepared);
  if (code != rpc::OK) {
    resp.set_code(code);
    return resp;
  }
  BufferKey key{req.client_id(), req.sequence()};
  auto size = buffer_.sizeOf(key);
  if (!size) {
    resp.set_code(rpc::DATA_MISSING);
    return resp;
  }
  if (req.offset() + *size > config_.chunk_size) {
    resp.set_code(rpc::OUT_OF_RANGE);
    return resp;
  }
  auto data = buffer_.take(key);
  if (!data) {
    resp.set_code(rpc::DATA_MISSING);
    return resp;
  }
  uint64_t serial = leases_.nextSerial(req.handle());
  if (store_->write(req.handle(), req.offset(), *data) != rpc::OK) {
    resp.set_code(rpc::FAILED);
    resp.set_failed_at(id_);
    return resp;
  }
  rpc::ApplyMutationRequest apply;
  apply.set_handle(req.handle());
  apply.set_version(req.version());
  apply.set_serial(serial);
  apply.set_kind(rpc::WRITE);
  apply.set_offset(req.offset());
  apply.set_client_id(req.client_id());
  apply.set_sequence(req.sequence());
  if (auto failed = forwardMutation(apply, prepared.lease.secondaries)) {
    resp.set_code(rpc::FAILED);
    resp.set_failed_at(*failed);
    return resp;
  }
  resp.set_code(rpc::OK);
  return resp;
}

rpc::RecordAppendResponse Chunkserver::recordAppend(const rpc::RecordAppendRequest& req) {
  rpc::RecordAppendResponse resp;
  Prepared prepared;
  rpc::ResultCode code = prepareMutation(req.handle(), req.version(), &prepared);
  if (code != rpc::OK) {
    resp.set_code(code);
    return resp;
  }
  auto data = buffer_.take({req.client_id(), req.sequence()});
  if (!data) {
    resp.set_code(rpc::DATA_MISSING);
    return resp;
  }
  uint64_t current = 0;
  if (store_->length(req.handle(), &current) != rpc::OK) {
    resp.set_code(rpc::NO_SUCH_CHUNK);
    return resp;
  }
  uint64_t serial = leases_.nextSerial(req.handle());
  rpc::ApplyMutationRequest apply;
  apply.set_handle(req.handle());
  apply.set_version(req.version());
  apply.set_serial(serial);
  apply.set_offset(current);
  apply.set_client_id(req.client_id());
  apply.set_sequence(req.sequence());

  if (current + data->size() > config_.chunk_size) {
    if (store_->pad(req.handle(), current) != rpc::OK) {
      resp.set_code(rpc::FAILED);
      resp.set_failed_at(id_);
      return resp;
    }
    apply.set_kind(rpc::PAD);
    if (auto failed = forwardMutation(apply, prepared.lease.secondaries)) {
      GFS_LOG_WARN << "padding chunk " << handleToHex(req.handle()) << " did not reach " << *failed;
    }
    resp.set_code(rpc::RETRY_NEXT_CHUNK);
    return resp;
  }

  if (store_->write(req.handle(), current, *data) != rpc::OK) {
    resp.set_code(rpc::FAILED);
    resp.set_failed_at(id_);
    return resp;
  }
  apply.set_kind(rpc::WRITE);
  if (auto failed = forwardMutation(apply, prepared.lease.secondaries)) {
    resp.set_code(rpc::FAILED);
    resp.set_failed_at(*failed);
    return resp;
  }
  resp.set_code(rpc::OK);
  resp.set_offset(current);
  return resp;
}

rpc::GetChunkLengthResponse Chunkserver::getChunkLength(const rpc::GetChunkLengthRequest& req) {
  rpc::GetChunkLengthResponse resp;
  uint64_t length = 0;
  resp.set_code(store_->length(req.handle(), &length));
  resp.set_length(length);
  return resp;
}

rpc::ApplyMutationResponse Chunkserver::applyMutation(const rpc::ApplyMutationRequest& req) {
  rpc::ApplyMutationResponse resp;
  MutationLock lock = store_->lockForMutation(req.handle());
  if (!lock) {
    resp.set_code(rpc::NO_SUCH_CHUNK);
    return resp;
  }
  auto current = store_->version(req.handle());
  if (!current || *current != req.version()) {
    resp.set_code(current ? rpc::STALE_VERSION : rpc::NO_SUCH_CHUNK);
    return resp;
  }
  if (req.kind() == rpc::PAD) {
    resp.set_code(store_->pad(req.handle(), req.offset()));
    return resp;
  }
  auto data = buffer_.take({req.client_id(), req.sequence()});
  if (!data) {
    resp.set_code(rpc::DATA_MISSING);
    return resp;
  }
  if (req.offset() + data->size() > config_.chunk_size) {
    resp.set_code(rpc::OUT_OF_RANGE);
    return resp;
  }
  resp.set_code(store_->write(req.handle(), req.offset(), *data));
  return resp;
}

rpc::CreateChunkResponse Chunkserver::createChunk(const rpc::CreateChunkRequest& req) {
  rpc::CreateChunkResponse resp;
  auto existing = store_->version(req.handle());
  if (existing) {
    resp.set_code(*existing == req.version() ? rpc::OK : rpc::FAILED);
    return resp;
  }
  rpc::ResultCode code = req.copy_from() != 0 ? store_->createCopy(req.handle(), req.version(), req.copy_from())
                                              : store_->create(req.handle(), req.version());
  if (code == rpc::FAILED) {
    auto raced = store_->version(req.handle());
    if (raced && *raced == req.version()) code = rpc::OK;
  }
  resp.set_code(code);
  return resp;
}

rpc::GrantLeaseResponse Chunkserver::grantLease(const rpc::GrantLeaseRequest& req) {
  rpc::GrantLeaseResponse resp;
  rpc::ResultCode code = store_->setVersion(req.handle(), req.version());
  if (code != rpc::OK) {
    resp.set_code(code);
    return resp;
  }
  leases_.grant(req.handle(), Millis(req.lease_ms()), {req.secondaries().begin(), req.secondaries().end()});
  resp.set_code(rpc::OK);
  return resp;
}

rpc::RevokeLeaseResponse Chunkserver::revokeLease(const rpc::RevokeLeaseRequest& req) {
  rpc::RevokeLeaseResponse resp;
  leases_.revoke(req.handle());
  resp.set_code(rpc::OK);
  return resp;
}

rpc::UpdateVersionResponse Chunkserver::updateVersion(const rpc::UpdateVersionRequest& req) {
  rpc::UpdateVersionResponse resp;
  rpc::ResultCode code = store_->setVersion(req.handle(), req.version());
  if (code == rpc::OK) leases_.revoke(req.handle());
  resp.set_code(code);
  return resp;
}

void Chunkserver::startHeartbeat() {
  std::lock_guard<std::mutex> lock(stop_mutex_);
  if (heartbeat_thread_.joinable()) return;
  stopping_ = false;
  heartbeat_thread_ = std::thread([this] { heartbeatLoop(); });
}

void Chunkserver::stop() {
  {
    std::lock_guard<std::mutex> lock(stop_mutex_);
    stopping_ = true;
  }
  stop_cv_.notify_all();
  if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
}

void Chunkserver::heartbeatLoop() {
  while (true) {
    sendHeartbeat();
    std::unique_lock<std::mutex> lock(stop_mutex_);
    if (stop_cv_.wait_for(lock, config_.heartbeat_interval, [this] { return stopping_; })) return;
  }
}

bool Chunkserver::sendHeartbeat() {
  rpc::HeartBeatRequest req;
  req.set_chunkserver_id(id_);
  {
    std::lock_guard<std::mutex> lock(advertise_mutex_);
    req.set_address(advertise_);
  }
  req.set_rack(config_.rack);
  for (const ChunkListing& c : store_->list()) {
    rpc::ChunkReport* report = req.add_chunks();
    report->set_handle(c.handle);
    report->set_version(c.version);
    report->set_length(c.length);
  }
  for (uint64_t h : leases_.heldHandles()) req.add_lease_extension_requests(h);
  std::vector<uint64_t> corrupt = store_->corruptHandles();
  for (uint64_t h : corrupt) req.add_corrupt(h);

  grpc::ClientContext ctx;
  ctx.set_deadline(deadlineAfter(config_.rpc_deadline));
  rpc::HeartBeatResponse resp;
  grpc::Status status = master_->HeartBeat(&ctx, req, &resp);
  if (!status.ok()) {
    if (master_reachable_) GFS_LOG_WARN << "master " << config_.master_address << " unreachable: " << status.error_message();
    master_reachable_ = false;
    return false;
  }
  if (!master_reachable_) GFS_LOG_INFO << "master " << config_.master_address << " reachable again";
  master_reachable_ = true;
  store_->clearCorrupt(corrupt);
  for (uint64_t h : resp.delete_handles()) {
    leases_.revoke(h);
    if (store_->remove(h)) GFS_LOG_INFO << "deleted chunk " << handleToHex(h) << " on master's word";
  }
  for (const rpc::LeaseExtension& ext : resp.extended()) leases_.extend(ext.handle(), Millis(ext.lease_ms()));
  return true;
}

}
