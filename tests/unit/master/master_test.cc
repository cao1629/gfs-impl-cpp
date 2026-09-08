#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <mutex>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "common/config.h"
#include "gfs.grpc.pb.h"
#include "master/checkpoint.h"
#include "master/master.h"
#include "master/master_service.h"

namespace gfs {

namespace fs = std::filesystem;

namespace {

void sleepMs(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

bool waitFor(const std::function<bool()>& pred, int timeout_ms) {
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    sleepMs(20);
  }
  return pred();
}

Config testConfig(const std::string& data_dir) {
  Config config;
  config.data_dir = data_dir;
  config.heartbeat_interval = Millis(100);
  config.chunkserver_dead_timeout = Millis(400);
  config.lease_duration = Millis(2000);
  config.lease_clock_skew_margin = Millis(50);
  config.gc_interval = Millis(200);
  config.deleted_file_retention = Millis(1000);
  config.rpc_deadline = Millis(500);
  config.log_flush_max_delay = Millis(2);
  config.master_worker_threads = 4;
  return config;
}

class FakeChunkserver final : public rpc::Chunkserver::Service {
 public:
  FakeChunkserver(std::string id, std::string rack) : id_(std::move(id)), rack_(std::move(rack)) {
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
    address_ = "127.0.0.1:" + std::to_string(port);
  }

  ~FakeChunkserver() override {
    stopHeartbeats();
    server_->Shutdown();
  }

  grpc::Status CreateChunk(grpc::ServerContext*, const rpc::CreateChunkRequest* req, rpc::CreateChunkResponse* resp) override {
    std::lock_guard<std::mutex> lock(mutex_);
    chunks_[req->handle()] = req->version();
    creates_.push_back(*req);
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

  grpc::Status GrantLease(grpc::ServerContext*, const rpc::GrantLeaseRequest* req, rpc::GrantLeaseResponse* resp) override {
    std::lock_guard<std::mutex> lock(mutex_);
    chunks_[req->handle()] = req->version();
    grants_.push_back(*req);
    held_leases_.insert(req->handle());
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

  grpc::Status RevokeLease(grpc::ServerContext*, const rpc::RevokeLeaseRequest* req, rpc::RevokeLeaseResponse* resp) override {
    std::lock_guard<std::mutex> lock(mutex_);
    revokes_.push_back(*req);
    held_leases_.erase(req->handle());
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

  grpc::Status UpdateVersion(grpc::ServerContext*, const rpc::UpdateVersionRequest* req, rpc::UpdateVersionResponse* resp) override {
    std::lock_guard<std::mutex> lock(mutex_);
    chunks_[req->handle()] = req->version();
    updates_.push_back(*req);
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

  rpc::HeartBeatResponse heartbeat(rpc::Master::Stub& master) {
    rpc::HeartBeatRequest req;
    req.set_chunkserver_id(id_);
    req.set_address(address_);
    req.set_rack(rack_);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (const auto& [handle, version] : chunks_) {
        rpc::ChunkReport* report = req.add_chunks();
        report->set_handle(handle);
        report->set_version(version);
      }
      for (uint64_t handle : held_leases_) req.add_lease_extension_requests(handle);
    }
    rpc::HeartBeatResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
    if (master.HeartBeat(&ctx, req, &resp).ok()) {
      std::lock_guard<std::mutex> lock(mutex_);
      for (uint64_t handle : resp.delete_handles()) chunks_.erase(handle);
    }
    return resp;
  }

  void startHeartbeats(const std::string& master_address, int interval_ms) {
    stopHeartbeats();
    running_ = true;
    heartbeat_thread_ = std::thread([this, master_address, interval_ms] {
      auto stub = rpc::Master::NewStub(grpc::CreateChannel(master_address, grpc::InsecureChannelCredentials()));
      while (running_) {
        heartbeat(*stub);
        for (int i = 0; i < interval_ms / 10 && running_; ++i) sleepMs(10);
      }
    });
  }

  void stopHeartbeats() {
    running_ = false;
    if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
  }

  void setVersion(uint64_t handle, uint64_t version) {
    std::lock_guard<std::mutex> lock(mutex_);
    chunks_[handle] = version;
  }

  bool holds(uint64_t handle) {
    std::lock_guard<std::mutex> lock(mutex_);
    return chunks_.count(handle) > 0;
  }

  size_t chunkCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    return chunks_.size();
  }

  std::vector<rpc::CreateChunkRequest> creates() { std::lock_guard<std::mutex> lock(mutex_); return creates_; }
  std::vector<rpc::GrantLeaseRequest> grants() { std::lock_guard<std::mutex> lock(mutex_); return grants_; }
  std::vector<rpc::RevokeLeaseRequest> revokes() { std::lock_guard<std::mutex> lock(mutex_); return revokes_; }
  std::vector<rpc::UpdateVersionRequest> updates() { std::lock_guard<std::mutex> lock(mutex_); return updates_; }

  const std::string& id() const { return id_; }
  const std::string& address() const { return address_; }

 private:
  std::string id_;
  std::string rack_;
  std::string address_;
  std::unique_ptr<grpc::Server> server_;
  std::mutex mutex_;
  std::map<uint64_t, uint64_t> chunks_;
  std::set<uint64_t> held_leases_;
  std::vector<rpc::CreateChunkRequest> creates_;
  std::vector<rpc::GrantLeaseRequest> grants_;
  std::vector<rpc::RevokeLeaseRequest> revokes_;
  std::vector<rpc::UpdateVersionRequest> updates_;
  std::atomic<bool> running_{false};
  std::thread heartbeat_thread_;
};

class MasterHarness {
 public:
  explicit MasterHarness(Config config) : config_(std::move(config)) { start(); }
  ~MasterHarness() { stop(); }

  void start() {
    master_ = std::make_unique<Master>(config_);
    master_->start();
    service_ = std::make_unique<MasterService>(*master_);
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    address_ = "127.0.0.1:" + std::to_string(port);
    stub_ = rpc::Master::NewStub(grpc::CreateChannel(address_, grpc::InsecureChannelCredentials()));
  }

  void stop() {
    if (server_) server_->Shutdown();
    server_.reset();
    stub_.reset();
    service_.reset();
    master_.reset();
  }

  void restart() {
    stop();
    start();
  }

  rpc::Master::Stub& stub() { return *stub_; }
  Master& master() { return *master_; }
  const std::string& address() const { return address_; }

  grpc::Status create(const std::string& path) {
    grpc::ClientContext ctx;
    rpc::CreateRequest req;
    req.set_path(path);
    rpc::CreateResponse resp;
    return stub_->Create(&ctx, req, &resp);
  }

  grpc::Status open(const std::string& path, uint64_t* chunk_count = nullptr) {
    grpc::ClientContext ctx;
    rpc::OpenRequest req;
    req.set_path(path);
    rpc::OpenResponse resp;
    auto status = stub_->Open(&ctx, req, &resp);
    if (chunk_count) *chunk_count = resp.chunk_count();
    return status;
  }

  grpc::Status remove(const std::string& path) {
    grpc::ClientContext ctx;
    rpc::DeleteRequest req;
    req.set_path(path);
    rpc::DeleteResponse resp;
    return stub_->Delete(&ctx, req, &resp);
  }

  grpc::Status rename(const std::string& source, const std::string& target) {
    grpc::ClientContext ctx;
    rpc::RenameRequest req;
    req.set_source(source);
    req.set_target(target);
    rpc::RenameResponse resp;
    return stub_->Rename(&ctx, req, &resp);
  }

  grpc::Status snapshot(const std::string& source, const std::string& target) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    rpc::SnapshotRequest req;
    req.set_source(source);
    req.set_target(target);
    rpc::SnapshotResponse resp;
    return stub_->Snapshot(&ctx, req, &resp);
  }

  std::vector<std::string> list(const std::string& directory, bool include_hidden = false, grpc::Status* status_out = nullptr) {
    grpc::ClientContext ctx;
    rpc::FindMatchingFilesRequest req;
    req.set_directory(directory);
    req.set_include_hidden(include_hidden);
    rpc::FindMatchingFilesResponse resp;
    auto status = stub_->FindMatchingFiles(&ctx, req, &resp);
    if (status_out) *status_out = status;
    std::vector<std::string> names;
    for (const auto& e : resp.entries()) names.push_back(e.name() + (e.is_directory() ? "/" : ""));
    return names;
  }

  rpc::AddChunkResponse addChunk(const std::string& path, uint64_t index, grpc::Status* status_out = nullptr) {
    grpc::ClientContext ctx;
    rpc::AddChunkRequest req;
    req.set_path(path);
    req.set_index(index);
    rpc::AddChunkResponse resp;
    auto status = stub_->AddChunk(&ctx, req, &resp);
    if (status_out) *status_out = status;
    return resp;
  }

  rpc::FindLocationResponse findLocation(const std::string& path, uint64_t index, grpc::Status* status_out = nullptr) {
    grpc::ClientContext ctx;
    rpc::FindLocationRequest req;
    req.set_path(path);
    req.set_first_index(index);
    req.set_count(1);
    rpc::FindLocationResponse resp;
    auto status = stub_->FindLocation(&ctx, req, &resp);
    if (status_out) *status_out = status;
    return resp;
  }

  rpc::FindLeaseHolderResponse findLeaseHolder(const std::string& path, uint64_t index, grpc::Status* status_out = nullptr) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    rpc::FindLeaseHolderRequest req;
    req.set_path(path);
    req.set_index(index);
    rpc::FindLeaseHolderResponse resp;
    auto status = stub_->FindLeaseHolder(&ctx, req, &resp);
    if (status_out) *status_out = status;
    return resp;
  }

 private:
  Config config_;
  std::unique_ptr<Master> master_;
  std::unique_ptr<MasterService> service_;
  std::unique_ptr<grpc::Server> server_;
  std::string address_;
  std::unique_ptr<rpc::Master::Stub> stub_;
};

class MasterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = (fs::temp_directory_path() / ("gfs-master-test-" + std::to_string(::getpid()) + "-" + ::testing::UnitTest::GetInstance()->current_test_info()->name())).string();
    fs::remove_all(dir_);
    fs::create_directories(dir_);
    harness_ = std::make_unique<MasterHarness>(testConfig(dir_));
  }

  void TearDown() override {
    for (auto& f : fakes_) f->stopHeartbeats();
    harness_.reset();
    fakes_.clear();
    fs::remove_all(dir_);
  }

  void startFakes(int count) {
    for (int i = 0; i < count; ++i) {
      fakes_.push_back(std::make_unique<FakeChunkserver>("cs" + std::to_string(i), "rack" + std::to_string(i % 2)));
      fakes_.back()->startHeartbeats(harness_->address(), 100);
    }
    sleepMs(250);
  }

  FakeChunkserver* fakeById(const std::string& id) {
    for (auto& f : fakes_) {
      if (f->id() == id) return f.get();
    }
    return nullptr;
  }

  std::string dir_;
  std::unique_ptr<MasterHarness> harness_;
  std::vector<std::unique_ptr<FakeChunkserver>> fakes_;
};

}

TEST_F(MasterTest, NamespaceOperations) {
  auto& h = *harness_;
  ASSERT_TRUE(h.create("/a/b").ok());
  ASSERT_TRUE(h.create("/a/c").ok());
  EXPECT_EQ(h.create("/a/b").error_code(), grpc::StatusCode::ALREADY_EXISTS);
  EXPECT_EQ(h.create("/a/b/x").error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(h.create("/a").error_code(), grpc::StatusCode::ALREADY_EXISTS);
  EXPECT_EQ(h.create("bad").error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  uint64_t count = 9;
  ASSERT_TRUE(h.open("/a/b", &count).ok());
  EXPECT_EQ(count, 0u);
  EXPECT_EQ(h.list("/"), std::vector<std::string>{"a/"});
  EXPECT_EQ(h.list("/a"), (std::vector<std::string>{"b", "c"}));
  grpc::Status status;
  h.list("/nope", false, &status);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);

  ASSERT_TRUE(h.remove("/a/b").ok());
  EXPECT_EQ(h.open("/a/b").error_code(), grpc::StatusCode::NOT_FOUND);
  EXPECT_EQ(h.list("/a"), std::vector<std::string>{"c"});
  auto hidden = h.list("/a", true);
  ASSERT_EQ(hidden.size(), 2u);
  EXPECT_EQ(hidden[0].rfind(".deleted.", 0), 0u);
  ASSERT_TRUE(h.rename("/a/" + hidden[0], "/a/b").ok());
  EXPECT_TRUE(h.open("/a/b").ok());
  ASSERT_TRUE(h.remove("/a/b").ok());
  hidden = h.list("/a", true);
  ASSERT_TRUE(h.remove("/a/" + hidden[0]).ok());
  EXPECT_EQ(h.list("/a", true), std::vector<std::string>{"c"});

  EXPECT_EQ(h.remove("/a").error_code(), grpc::StatusCode::NOT_FOUND);
  EXPECT_EQ(h.remove("/missing").error_code(), grpc::StatusCode::NOT_FOUND);
  ASSERT_TRUE(h.rename("/a/c", "/z/c").ok());
  ASSERT_TRUE(h.rename("/z", "/y").ok());
  EXPECT_TRUE(h.open("/y/c").ok());
  EXPECT_EQ(h.rename("/y", "/y/inside").error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(h.rename("/y/c", "/y/c").error_code(), grpc::StatusCode::ALREADY_EXISTS);
  EXPECT_EQ(h.list("/"), std::vector<std::string>{"y/"});
}

TEST_F(MasterTest, AddChunkAndLeaseGrant) {
  startFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.create("/f").ok());
  grpc::Status status;
  auto added = h.addChunk("/f", 0, &status);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(added.code(), rpc::OK);
  EXPECT_EQ(added.chunk().replicas_size(), 3);
  EXPECT_EQ(added.chunk().version(), 1u);
  uint64_t handle = added.chunk().handle();
  for (auto& f : fakes_) {
    ASSERT_EQ(f->creates().size(), 1u);
    EXPECT_EQ(f->creates()[0].handle(), handle);
    EXPECT_EQ(f->creates()[0].version(), 1u);
    EXPECT_EQ(f->creates()[0].copy_from(), 0u);
  }
  EXPECT_EQ(h.addChunk("/f", 0).chunk().handle(), handle);
  h.addChunk("/f", 5, &status);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  uint64_t count = 0;
  ASSERT_TRUE(h.open("/f", &count).ok());
  EXPECT_EQ(count, 1u);

  auto located = h.findLocation("/f", 0);
  ASSERT_EQ(located.chunks_size(), 1);
  EXPECT_EQ(located.chunks(0).replicas_size(), 3);

  auto lease = h.findLeaseHolder("/f", 0, &status);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(lease.code(), rpc::OK);
  EXPECT_EQ(lease.handle(), handle);
  EXPECT_EQ(lease.version(), 2u);
  EXPECT_EQ(lease.secondaries_size(), 2);
  FakeChunkserver* primary = fakeById(lease.primary().chunkserver_id());
  ASSERT_NE(primary, nullptr);
  ASSERT_EQ(primary->grants().size(), 1u);
  EXPECT_EQ(primary->grants()[0].version(), 2u);
  EXPECT_EQ(primary->grants()[0].secondaries_size(), 2);
  EXPECT_EQ(primary->grants()[0].lease_ms(), 2000u);
  for (const auto& s : lease.secondaries()) {
    FakeChunkserver* secondary = fakeById(s.chunkserver_id());
    ASSERT_NE(secondary, nullptr);
    ASSERT_EQ(secondary->updates().size(), 1u);
    EXPECT_EQ(secondary->updates()[0].version(), 2u);
  }
  auto again = h.findLeaseHolder("/f", 0);
  EXPECT_EQ(again.primary().chunkserver_id(), lease.primary().chunkserver_id());
  EXPECT_EQ(again.version(), 2u);
  EXPECT_EQ(primary->grants().size(), 1u);
  EXPECT_EQ(h.findLocation("/f", 0).chunks(0).version(), 2u);

  sleepMs(300);
  auto& state = h.master().state();
  std::lock_guard<std::mutex> lock(state.mutex);
  ASSERT_TRUE(state.chunks.find(handle)->lease.has_value());
  EXPECT_GT(state.chunks.find(handle)->lease->expiry, now() + Millis(1500));
}

TEST_F(MasterTest, StaleReplicaIsExcludedAndToldToDelete) {
  startFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.create("/s").ok());
  uint64_t handle = h.addChunk("/s", 0).chunk().handle();
  auto lease = h.findLeaseHolder("/s", 0);
  ASSERT_EQ(lease.code(), rpc::OK);
  FakeChunkserver* stale = fakeById(lease.secondaries(0).chunkserver_id());
  stale->setVersion(handle, 1);
  ASSERT_TRUE(waitFor([&] { return h.findLocation("/s", 0).chunks(0).replicas_size() == 2; }, 2000));
  auto located = h.findLocation("/s", 0);
  for (const auto& r : located.chunks(0).replicas()) EXPECT_NE(r.chunkserver_id(), stale->id());
  EXPECT_TRUE(waitFor([&] { return !stale->holds(handle); }, 2000));
  FakeChunkserver* primary = fakeById(lease.primary().chunkserver_id());
  EXPECT_TRUE(waitFor([&] { return primary->grants().size() >= 2; }, 2000));
  EXPECT_EQ(primary->grants().back().secondaries_size(), 1);
}

TEST_F(MasterTest, DeadChunkserverShrinksReplicaSetAndPrimaryDeathWaitsForExpiry) {
  startFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.create("/d").ok());
  h.addChunk("/d", 0);
  auto lease = h.findLeaseHolder("/d", 0);
  ASSERT_EQ(lease.code(), rpc::OK);
  FakeChunkserver* primary = fakeById(lease.primary().chunkserver_id());
  FakeChunkserver* secondary = fakeById(lease.secondaries(0).chunkserver_id());
  secondary->stopHeartbeats();
  ASSERT_TRUE(waitFor([&] { return h.findLocation("/d", 0).chunks(0).replicas_size() == 2; }, 3000));
  ASSERT_TRUE(waitFor([&] { return primary->grants().size() >= 2; }, 2000));
  EXPECT_EQ(primary->grants().back().version(), 3u);
  EXPECT_EQ(primary->grants().back().secondaries_size(), 1);
  EXPECT_EQ(h.findLeaseHolder("/d", 0).version(), 3u);

  primary->stopHeartbeats();
  ASSERT_TRUE(waitFor([&] { return h.findLocation("/d", 0).chunks(0).replicas_size() == 1; }, 3000));
  auto started = std::chrono::steady_clock::now();
  auto replacement = h.findLeaseHolder("/d", 0);
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started);
  ASSERT_EQ(replacement.code(), rpc::OK);
  EXPECT_NE(replacement.primary().chunkserver_id(), primary->id());
  EXPECT_EQ(replacement.version(), 4u);
  EXPECT_GT(elapsed.count(), 500);
  EXPECT_LT(elapsed.count(), 3000);
}

TEST_F(MasterTest, SnapshotRevokesThenCopiesOnWrite) {
  startFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.create("/dir/s").ok());
  uint64_t old_handle = h.addChunk("/dir/s", 0).chunk().handle();
  auto lease = h.findLeaseHolder("/dir/s", 0);
  ASSERT_EQ(lease.code(), rpc::OK);
  FakeChunkserver* primary = fakeById(lease.primary().chunkserver_id());

  auto started = std::chrono::steady_clock::now();
  ASSERT_TRUE(h.snapshot("/dir", "/copy").ok());
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started);
  EXPECT_LT(elapsed.count(), 1000);
  ASSERT_EQ(primary->revokes().size(), 1u);
  EXPECT_EQ(primary->revokes()[0].handle(), old_handle);
  EXPECT_EQ(h.snapshot("/dir", "/copy").error_code(), grpc::StatusCode::ALREADY_EXISTS);
  EXPECT_EQ(h.list("/copy"), std::vector<std::string>{"s"});
  EXPECT_EQ(h.findLocation("/copy/s", 0).chunks(0).handle(), old_handle);

  auto cow = h.findLeaseHolder("/dir/s", 0);
  ASSERT_EQ(cow.code(), rpc::OK);
  EXPECT_NE(cow.handle(), old_handle);
  EXPECT_EQ(cow.version(), 2u);
  EXPECT_EQ(h.findLocation("/dir/s", 0).chunks(0).handle(), cow.handle());
  EXPECT_EQ(h.findLocation("/copy/s", 0).chunks(0).handle(), old_handle);
  for (auto& f : fakes_) {
    auto creates = f->creates();
    ASSERT_EQ(creates.size(), 2u);
    EXPECT_EQ(creates[1].handle(), cow.handle());
    EXPECT_EQ(creates[1].copy_from(), old_handle);
  }
  auto copy_lease = h.findLeaseHolder("/copy/s", 0);
  ASSERT_EQ(copy_lease.code(), rpc::OK);
  EXPECT_EQ(copy_lease.handle(), old_handle);
  EXPECT_EQ(copy_lease.version(), 3u);
  for (auto& f : fakes_) EXPECT_EQ(f->creates().size(), 2u);
}

TEST_F(MasterTest, RecoveryReplaysLogAndReconnectsReplicas) {
  startFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.create("/r/a").ok());
  ASSERT_TRUE(h.create("/r/b").ok());
  uint64_t handle = h.addChunk("/r/a", 0).chunk().handle();
  uint64_t second = h.addChunk("/r/a", 1).chunk().handle();
  auto lease = h.findLeaseHolder("/r/a", 0);
  ASSERT_EQ(lease.version(), 2u);
  ASSERT_TRUE(h.rename("/r/b", "/r/c").ok());
  ASSERT_TRUE(h.remove("/r/c").ok());

  for (auto& f : fakes_) f->stopHeartbeats();
  h.restart();
  uint64_t count = 0;
  ASSERT_TRUE(h.open("/r/a", &count).ok());
  EXPECT_EQ(count, 2u);
  EXPECT_EQ(h.open("/r/c").error_code(), grpc::StatusCode::NOT_FOUND);
  ASSERT_EQ(h.list("/r", true).size(), 2u);
  auto located = h.findLocation("/r/a", 0);
  ASSERT_EQ(located.chunks_size(), 1);
  EXPECT_EQ(located.chunks(0).handle(), handle);
  EXPECT_EQ(located.chunks(0).version(), 2u);
  EXPECT_EQ(located.chunks(0).replicas_size(), 0);
  EXPECT_EQ(h.findLeaseHolder("/r/a", 0).code(), rpc::NO_REPLICAS);

  for (auto& f : fakes_) f->startHeartbeats(h.address(), 100);
  ASSERT_TRUE(waitFor([&] { return h.findLocation("/r/a", 0).chunks(0).replicas_size() == 3; }, 3000));
  EXPECT_EQ(h.findLocation("/r/a", 1).chunks(0).handle(), second);
  ASSERT_TRUE(h.create("/r/new").ok());
  EXPECT_GT(h.addChunk("/r/new", 0).chunk().handle(), second);
  auto regranted = h.findLeaseHolder("/r/a", 0);
  ASSERT_EQ(regranted.code(), rpc::OK);
  EXPECT_EQ(regranted.version(), 3u);
}

TEST_F(MasterTest, GarbageCollectionRemovesHiddenFilesAndOrphanChunks) {
  startFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.create("/g").ok());
  uint64_t handle = h.addChunk("/g", 0).chunk().handle();
  for (auto& f : fakes_) ASSERT_TRUE(f->holds(handle));
  ASSERT_TRUE(h.remove("/g").ok());
  EXPECT_EQ(h.list("/", true).size(), 1u);
  ASSERT_TRUE(waitFor([&] { return h.list("/", true).empty(); }, 4000));
  ASSERT_TRUE(waitFor([&] {
    std::lock_guard<std::mutex> lock(h.master().state().mutex);
    return h.master().state().chunks.find(handle) == nullptr;
  }, 2000));
  for (auto& f : fakes_) EXPECT_TRUE(waitFor([&] { return !f->holds(handle); }, 2000));
}

TEST_F(MasterTest, CheckpointRotationSurvivesRestart) {
  harness_.reset();
  Config config = testConfig(dir_);
  config.checkpoint_log_threshold = 512;
  harness_ = std::make_unique<MasterHarness>(config);
  auto& h = *harness_;
  for (int i = 0; i < 60; ++i) ASSERT_TRUE(h.create("/many/f" + std::to_string(i)).ok());
  ASSERT_TRUE(waitFor([&] { return !Checkpointer::list(dir_).empty(); }, 3000));
  h.restart();
  EXPECT_EQ(h.list("/many").size(), 60u);
  ASSERT_TRUE(h.create("/many/after").ok());
  h.restart();
  EXPECT_EQ(h.list("/many").size(), 61u);
}

}
