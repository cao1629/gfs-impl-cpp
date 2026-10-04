#include "master/master.h"

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <mutex>
#include <thread>

#include "common/config.h"
#include "gfs.grpc.pb.h"
#include "master/checkpoint.h"
#include "master/master_service.h"

namespace gfs {

namespace fs = std::filesystem;

namespace {

void SleepMs(int ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

bool WaitFor(const std::function<bool()>& pred, int timeout_ms) {
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    SleepMs(20);
  }
  return pred();
}

Config TestConfig(const std::string& data_dir) {
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
  FakeChunkserver(std::string id, std::string rack)
      : id_(std::move(id)), rack_(std::move(rack)) {
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
    address_ = "127.0.0.1:" + std::to_string(port);
  }

  ~FakeChunkserver() override {
    StopHeartbeats();
    server_->Shutdown();
  }

  grpc::Status CreateChunk(grpc::ServerContext*,
                           const rpc::CreateChunkRequest* req,
                           rpc::CreateChunkResponse* resp) override {
    std::lock_guard<std::mutex> lock(mutex_);
    chunks_[req->handle()] = req->version();
    creates_.push_back(*req);
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

  grpc::Status GrantLease(grpc::ServerContext*,
                          const rpc::GrantLeaseRequest* req,
                          rpc::GrantLeaseResponse* resp) override {
    std::lock_guard<std::mutex> lock(mutex_);
    chunks_[req->handle()] = req->version();
    grants_.push_back(*req);
    held_leases_.insert(req->handle());
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

  grpc::Status RevokeLease(grpc::ServerContext*,
                           const rpc::RevokeLeaseRequest* req,
                           rpc::RevokeLeaseResponse* resp) override {
    Millis delay{0};
    {
      std::lock_guard<std::mutex> lock(mutex_);
      revokes_.push_back(*req);
      held_leases_.erase(req->handle());
      delay = revoke_delay_;
    }
    std::this_thread::sleep_for(delay);
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

  void DelayRevokes(Millis delay) {
    std::lock_guard<std::mutex> lock(mutex_);
    revoke_delay_ = delay;
  }

  grpc::Status UpdateVersion(grpc::ServerContext*,
                             const rpc::UpdateVersionRequest* req,
                             rpc::UpdateVersionResponse* resp) override {
    std::lock_guard<std::mutex> lock(mutex_);
    updates_.push_back(*req);
    if (fail_updates_) {
      resp->set_code(rpc::FAILED);
      return grpc::Status::OK;
    }
    chunks_[req->handle()] = req->version();
    resp->set_code(rpc::OK);
    return grpc::Status::OK;
  }

  void FailUpdates(bool fail) {
    std::lock_guard<std::mutex> lock(mutex_);
    fail_updates_ = fail;
  }

  rpc::HeartBeatResponse Heartbeat(rpc::Master::Stub& master) {
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
      for (uint64_t handle : held_leases_)
        req.add_lease_extension_requests(handle);
    }
    rpc::HeartBeatResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::milliseconds(500));
    if (master.HeartBeat(&ctx, req, &resp).ok()) {
      std::lock_guard<std::mutex> lock(mutex_);
      for (uint64_t handle : resp.delete_handles()) chunks_.erase(handle);
    }
    return resp;
  }

  void StartHeartbeats(const std::string& master_address, int interval_ms) {
    StopHeartbeats();
    running_ = true;
    heartbeat_thread_ = std::thread([this, master_address, interval_ms] {
      auto stub = rpc::Master::NewStub(grpc::CreateChannel(
          master_address, grpc::InsecureChannelCredentials()));
      while (running_) {
        Heartbeat(*stub);
        for (int i = 0; i < interval_ms / 10 && running_; ++i) SleepMs(10);
      }
    });
  }

  void StopHeartbeats() {
    running_ = false;
    if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
  }

  void SetVersion(uint64_t handle, uint64_t version) {
    std::lock_guard<std::mutex> lock(mutex_);
    chunks_[handle] = version;
  }

  bool Holds(uint64_t handle) {
    std::lock_guard<std::mutex> lock(mutex_);
    return chunks_.count(handle) > 0;
  }

  size_t ChunkCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    return chunks_.size();
  }

  std::vector<rpc::CreateChunkRequest> Creates() {
    std::lock_guard<std::mutex> lock(mutex_);
    return creates_;
  }
  std::vector<rpc::GrantLeaseRequest> Grants() {
    std::lock_guard<std::mutex> lock(mutex_);
    return grants_;
  }
  std::vector<rpc::RevokeLeaseRequest> Revokes() {
    std::lock_guard<std::mutex> lock(mutex_);
    return revokes_;
  }
  std::vector<rpc::UpdateVersionRequest> Updates() {
    std::lock_guard<std::mutex> lock(mutex_);
    return updates_;
  }

  const std::string& Id() const { return id_; }
  const std::string& Address() const { return address_; }

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
  bool fail_updates_ = false;
  Millis revoke_delay_{0};
  std::atomic<bool> running_{false};
  std::thread heartbeat_thread_;
};

class MasterHarness {
 public:
  explicit MasterHarness(Config config) : config_(std::move(config)) {
    Start();
  }
  ~MasterHarness() { Stop(); }

  void Start() {
    master_ = std::make_unique<Master>(config_);
    master_->Start();
    service_ = std::make_unique<MasterService>(*master_);
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    address_ = "127.0.0.1:" + std::to_string(port);
    stub_ = rpc::Master::NewStub(
        grpc::CreateChannel(address_, grpc::InsecureChannelCredentials()));
  }

  void Stop() {
    if (server_) server_->Shutdown();
    server_.reset();
    stub_.reset();
    service_.reset();
    master_.reset();
  }

  void Restart() {
    Stop();
    Start();
  }

  rpc::Master::Stub& Stub() { return *stub_; }
  // NOLINTNEXTLINE(readability-identifier-naming)
  Master& master() { return *master_; }
  const std::string& Address() const { return address_; }

  grpc::Status Create(const std::string& path) {
    grpc::ClientContext ctx;
    rpc::CreateRequest req;
    req.set_path(path);
    rpc::CreateResponse resp;
    return stub_->Create(&ctx, req, &resp);
  }

  grpc::Status Open(const std::string& path, uint64_t* chunk_count = nullptr) {
    grpc::ClientContext ctx;
    rpc::OpenRequest req;
    req.set_path(path);
    rpc::OpenResponse resp;
    auto status = stub_->Open(&ctx, req, &resp);
    if (chunk_count) *chunk_count = resp.chunk_count();
    return status;
  }

  grpc::Status Remove(const std::string& path) {
    grpc::ClientContext ctx;
    rpc::DeleteRequest req;
    req.set_path(path);
    rpc::DeleteResponse resp;
    return stub_->Delete(&ctx, req, &resp);
  }

  grpc::Status Rename(const std::string& source, const std::string& target) {
    grpc::ClientContext ctx;
    rpc::RenameRequest req;
    req.set_source(source);
    req.set_target(target);
    rpc::RenameResponse resp;
    return stub_->Rename(&ctx, req, &resp);
  }

  grpc::Status Snapshot(const std::string& source, const std::string& target) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(10));
    rpc::SnapshotRequest req;
    req.set_source(source);
    req.set_target(target);
    rpc::SnapshotResponse resp;
    return stub_->Snapshot(&ctx, req, &resp);
  }

  std::vector<std::string> List(const std::string& directory,
                                bool include_hidden = false,
                                grpc::Status* status_out = nullptr) {
    grpc::ClientContext ctx;
    rpc::FindMatchingFilesRequest req;
    req.set_directory(directory);
    req.set_include_hidden(include_hidden);
    rpc::FindMatchingFilesResponse resp;
    auto status = stub_->FindMatchingFiles(&ctx, req, &resp);
    if (status_out) *status_out = status;
    std::vector<std::string> names;
    for (const auto& e : resp.entries())
      names.push_back(e.name() + (e.is_directory() ? "/" : ""));
    return names;
  }

  rpc::AddChunkResponse AddChunk(const std::string& path, uint64_t index,
                                 grpc::Status* status_out = nullptr) {
    grpc::ClientContext ctx;
    rpc::AddChunkRequest req;
    req.set_path(path);
    req.set_index(index);
    rpc::AddChunkResponse resp;
    auto status = stub_->AddChunk(&ctx, req, &resp);
    if (status_out) *status_out = status;
    return resp;
  }

  rpc::FindLocationResponse FindLocation(const std::string& path,
                                         uint64_t index,
                                         grpc::Status* status_out = nullptr) {
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

  rpc::FindLeaseHolderResponse FindLeaseHolder(
      const std::string& path, uint64_t index,
      grpc::Status* status_out = nullptr) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(10));
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
    dir_ = (fs::temp_directory_path() /
            ("gfs-master-test-" + std::to_string(::getpid()) + "-" +
             ::testing::UnitTest::GetInstance()->current_test_info()->name()))
               .string();
    fs::remove_all(dir_);
    fs::create_directories(dir_);
    harness_ = std::make_unique<MasterHarness>(TestConfig(dir_));
  }

  void TearDown() override {
    for (auto& f : fakes_) f->StopHeartbeats();
    harness_.reset();
    fakes_.clear();
    fs::remove_all(dir_);
  }

  void StartFakes(int count) {
    for (int i = 0; i < count; ++i) {
      fakes_.push_back(std::make_unique<FakeChunkserver>(
          "cs" + std::to_string(i), "rack" + std::to_string(i % 2)));
      fakes_.back()->StartHeartbeats(harness_->Address(), 100);
    }
    SleepMs(250);
  }

  FakeChunkserver* FakeById(const std::string& id) {
    for (auto& f : fakes_) {
      if (f->Id() == id) return f.get();
    }
    return nullptr;
  }

  std::string dir_;
  std::unique_ptr<MasterHarness> harness_;
  std::vector<std::unique_ptr<FakeChunkserver>> fakes_;
};

}  // namespace

TEST_F(MasterTest, NamespaceOperations) {
  auto& h = *harness_;
  ASSERT_TRUE(h.Create("/a/b").ok());
  ASSERT_TRUE(h.Create("/a/c").ok());
  EXPECT_EQ(h.Create("/a/b").error_code(), grpc::StatusCode::ALREADY_EXISTS);
  EXPECT_EQ(h.Create("/a/b/x").error_code(),
            grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(h.Create("/a").error_code(), grpc::StatusCode::ALREADY_EXISTS);
  EXPECT_EQ(h.Create("bad").error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  uint64_t count = 9;
  ASSERT_TRUE(h.Open("/a/b", &count).ok());
  EXPECT_EQ(count, 0u);
  EXPECT_EQ(h.List("/"), std::vector<std::string>{"a/"});
  EXPECT_EQ(h.List("/a"), (std::vector<std::string>{"b", "c"}));
  grpc::Status status;
  h.List("/nope", false, &status);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);

  ASSERT_TRUE(h.Remove("/a/b").ok());
  EXPECT_EQ(h.Open("/a/b").error_code(), grpc::StatusCode::NOT_FOUND);
  EXPECT_EQ(h.List("/a"), std::vector<std::string>{"c"});
  auto hidden = h.List("/a", true);
  ASSERT_EQ(hidden.size(), 2u);
  EXPECT_EQ(hidden[0].rfind(".deleted.", 0), 0u);
  ASSERT_TRUE(h.Rename("/a/" + hidden[0], "/a/b").ok());
  EXPECT_TRUE(h.Open("/a/b").ok());
  ASSERT_TRUE(h.Remove("/a/b").ok());
  hidden = h.List("/a", true);
  ASSERT_TRUE(h.Remove("/a/" + hidden[0]).ok());
  EXPECT_EQ(h.List("/a", true), std::vector<std::string>{"c"});

  EXPECT_EQ(h.Remove("/a").error_code(), grpc::StatusCode::NOT_FOUND);
  EXPECT_EQ(h.Remove("/missing").error_code(), grpc::StatusCode::NOT_FOUND);
  ASSERT_TRUE(h.Rename("/a/c", "/z/c").ok());
  ASSERT_TRUE(h.Rename("/z", "/y").ok());
  EXPECT_TRUE(h.Open("/y/c").ok());
  EXPECT_EQ(h.Rename("/y", "/y/inside").error_code(),
            grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(h.Rename("/y/c", "/y/c").error_code(),
            grpc::StatusCode::ALREADY_EXISTS);
  EXPECT_EQ(h.List("/"), std::vector<std::string>{"y/"});
}

TEST_F(MasterTest, AddChunkAndLeaseGrant) {
  StartFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.Create("/f").ok());
  grpc::Status status;
  auto added = h.AddChunk("/f", 0, &status);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(added.code(), rpc::OK);
  EXPECT_EQ(added.chunk().replicas_size(), 3);
  EXPECT_EQ(added.chunk().version(), 1u);
  uint64_t handle = added.chunk().handle();
  for (auto& f : fakes_) {
    ASSERT_EQ(f->Creates().size(), 1u);
    EXPECT_EQ(f->Creates()[0].handle(), handle);
    EXPECT_EQ(f->Creates()[0].version(), 1u);
    EXPECT_EQ(f->Creates()[0].copy_from(), 0u);
  }
  EXPECT_EQ(h.AddChunk("/f", 0).chunk().handle(), handle);
  h.AddChunk("/f", 5, &status);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  uint64_t count = 0;
  ASSERT_TRUE(h.Open("/f", &count).ok());
  EXPECT_EQ(count, 1u);

  auto located = h.FindLocation("/f", 0);
  ASSERT_EQ(located.chunks_size(), 1);
  EXPECT_EQ(located.chunks(0).replicas_size(), 3);

  auto lease = h.FindLeaseHolder("/f", 0, &status);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(lease.code(), rpc::OK);
  EXPECT_EQ(lease.handle(), handle);
  EXPECT_EQ(lease.version(), 2u);
  EXPECT_EQ(lease.secondaries_size(), 2);
  FakeChunkserver* primary = FakeById(lease.primary().chunkserver_id());
  ASSERT_NE(primary, nullptr);
  ASSERT_EQ(primary->Grants().size(), 1u);
  EXPECT_EQ(primary->Grants()[0].version(), 2u);
  EXPECT_EQ(primary->Grants()[0].secondaries_size(), 2);
  EXPECT_EQ(primary->Grants()[0].lease_ms(), 2000u);
  for (const auto& s : lease.secondaries()) {
    FakeChunkserver* secondary = FakeById(s.chunkserver_id());
    ASSERT_NE(secondary, nullptr);
    ASSERT_EQ(secondary->Updates().size(), 1u);
    EXPECT_EQ(secondary->Updates()[0].version(), 2u);
  }
  auto again = h.FindLeaseHolder("/f", 0);
  EXPECT_EQ(again.primary().chunkserver_id(), lease.primary().chunkserver_id());
  EXPECT_EQ(again.version(), 2u);
  EXPECT_EQ(primary->Grants().size(), 1u);
  EXPECT_EQ(h.FindLocation("/f", 0).chunks(0).version(), 2u);

  SleepMs(300);
  auto& state = h.master().State();
  std::lock_guard<std::mutex> lock(state.mutex);
  ASSERT_TRUE(state.chunks.Find(handle)->lease.has_value());
  EXPECT_GT(state.chunks.Find(handle)->lease->expiry, Now() + Millis(1500));
}

TEST_F(MasterTest, GrantResendsThePrimaryAListWithoutAFailedSecondary) {
  StartFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.Create("/f").ok());
  ASSERT_EQ(h.AddChunk("/f", 0).code(), rpc::OK);
  FakeChunkserver* failing = FakeById("cs2");
  ASSERT_NE(failing, nullptr);
  failing->StopHeartbeats();
  failing->FailUpdates(true);

  grpc::Status status;
  auto lease = h.FindLeaseHolder("/f", 0, &status);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(lease.code(), rpc::OK);
  EXPECT_EQ(lease.version(), 2u);
  ASSERT_EQ(lease.secondaries_size(), 1);
  EXPECT_NE(lease.secondaries(0).chunkserver_id(), failing->Id());
  FakeChunkserver* primary = FakeById(lease.primary().chunkserver_id());
  ASSERT_NE(primary, nullptr);
  ASSERT_NE(primary, failing);
  auto grants = primary->Grants();
  ASSERT_EQ(grants.size(), 2u);
  EXPECT_EQ(grants[0].secondaries_size(), 2);
  EXPECT_EQ(grants[1].version(), 2u);
  ASSERT_EQ(grants[1].secondaries_size(), 1);
  EXPECT_EQ(grants[1].secondaries(0).chunkserver_id(),
            lease.secondaries(0).chunkserver_id());
}

TEST_F(MasterTest, StaleReplicaIsExcludedAndToldToDelete) {
  StartFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.Create("/s").ok());
  uint64_t handle = h.AddChunk("/s", 0).chunk().handle();
  auto lease = h.FindLeaseHolder("/s", 0);
  ASSERT_EQ(lease.code(), rpc::OK);
  FakeChunkserver* stale = FakeById(lease.secondaries(0).chunkserver_id());
  stale->SetVersion(handle, 1);
  ASSERT_TRUE(WaitFor(
      [&] { return h.FindLocation("/s", 0).chunks(0).replicas_size() == 2; },
      2000));
  auto located = h.FindLocation("/s", 0);
  for (const auto& r : located.chunks(0).replicas())
    EXPECT_NE(r.chunkserver_id(), stale->Id());
  EXPECT_TRUE(WaitFor([&] { return !stale->Holds(handle); }, 2000));
  FakeChunkserver* primary = FakeById(lease.primary().chunkserver_id());
  EXPECT_TRUE(WaitFor([&] { return primary->Grants().size() >= 2; }, 2000));
  EXPECT_EQ(primary->Grants().back().secondaries_size(), 1);
}

TEST_F(MasterTest,
       DeadChunkserverShrinksReplicaSetAndPrimaryDeathWaitsForExpiry) {
  StartFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.Create("/d").ok());
  h.AddChunk("/d", 0);
  auto lease = h.FindLeaseHolder("/d", 0);
  ASSERT_EQ(lease.code(), rpc::OK);
  FakeChunkserver* primary = FakeById(lease.primary().chunkserver_id());
  FakeChunkserver* secondary = FakeById(lease.secondaries(0).chunkserver_id());
  secondary->StopHeartbeats();
  ASSERT_TRUE(WaitFor(
      [&] { return h.FindLocation("/d", 0).chunks(0).replicas_size() == 2; },
      3000));
  ASSERT_TRUE(WaitFor([&] { return primary->Grants().size() >= 2; }, 2000));
  EXPECT_EQ(primary->Grants().back().version(), 3u);
  EXPECT_EQ(primary->Grants().back().secondaries_size(), 1);
  EXPECT_EQ(h.FindLeaseHolder("/d", 0).version(), 3u);

  primary->StopHeartbeats();
  ASSERT_TRUE(WaitFor(
      [&] { return h.FindLocation("/d", 0).chunks(0).replicas_size() == 1; },
      3000));
  auto started = std::chrono::steady_clock::now();
  auto replacement = h.FindLeaseHolder("/d", 0);
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - started);
  ASSERT_EQ(replacement.code(), rpc::OK);
  EXPECT_NE(replacement.primary().chunkserver_id(), primary->Id());
  EXPECT_EQ(replacement.version(), 4u);
  EXPECT_GT(elapsed.count(), 500);
  EXPECT_LT(elapsed.count(), 3000);
}

TEST_F(MasterTest, SnapshotStopsRevokingAtAPrimaryThatDidNotAnswer) {
  StartFakes(3);
  auto& h = *harness_;
  std::set<std::string> primaries;
  for (const char* name : {"a", "b", "c"}) {
    std::string path = std::string("/dir/") + name;
    ASSERT_TRUE(h.Create(path).ok());
    ASSERT_EQ(h.AddChunk(path, 0).code(), rpc::OK);
    auto lease = h.FindLeaseHolder(path, 0);
    ASSERT_EQ(lease.code(), rpc::OK);
    primaries.insert(lease.primary().chunkserver_id());
  }
  ASSERT_EQ(primaries.size(), 1u);
  FakeChunkserver* primary = FakeById(*primaries.begin());
  ASSERT_NE(primary, nullptr);
  primary->DelayRevokes(Millis(1500));

  ASSERT_TRUE(h.Snapshot("/dir", "/copy").ok());
  EXPECT_EQ(primary->Revokes().size(), 1u);
}

TEST_F(MasterTest, SnapshotRevokesThenCopiesOnWrite) {
  StartFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.Create("/dir/s").ok());
  uint64_t old_handle = h.AddChunk("/dir/s", 0).chunk().handle();
  auto lease = h.FindLeaseHolder("/dir/s", 0);
  ASSERT_EQ(lease.code(), rpc::OK);
  FakeChunkserver* primary = FakeById(lease.primary().chunkserver_id());

  auto started = std::chrono::steady_clock::now();
  ASSERT_TRUE(h.Snapshot("/dir", "/copy").ok());
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - started);
  EXPECT_LT(elapsed.count(), 1000);
  ASSERT_EQ(primary->Revokes().size(), 1u);
  EXPECT_EQ(primary->Revokes()[0].handle(), old_handle);
  EXPECT_EQ(h.Snapshot("/dir", "/copy").error_code(),
            grpc::StatusCode::ALREADY_EXISTS);
  EXPECT_EQ(h.List("/copy"), std::vector<std::string>{"s"});
  EXPECT_EQ(h.FindLocation("/copy/s", 0).chunks(0).handle(), old_handle);

  auto cow = h.FindLeaseHolder("/dir/s", 0);
  ASSERT_EQ(cow.code(), rpc::OK);
  EXPECT_NE(cow.handle(), old_handle);
  EXPECT_EQ(cow.version(), 2u);
  EXPECT_EQ(h.FindLocation("/dir/s", 0).chunks(0).handle(), cow.handle());
  EXPECT_EQ(h.FindLocation("/copy/s", 0).chunks(0).handle(), old_handle);
  for (auto& f : fakes_) {
    auto creates = f->Creates();
    ASSERT_EQ(creates.size(), 2u);
    EXPECT_EQ(creates[1].handle(), cow.handle());
    EXPECT_EQ(creates[1].copy_from(), old_handle);
  }
  auto copy_lease = h.FindLeaseHolder("/copy/s", 0);
  ASSERT_EQ(copy_lease.code(), rpc::OK);
  EXPECT_EQ(copy_lease.handle(), old_handle);
  EXPECT_EQ(copy_lease.version(), 3u);
  for (auto& f : fakes_) EXPECT_EQ(f->Creates().size(), 2u);
}

TEST_F(MasterTest, RecoveryReplaysLogAndReconnectsReplicas) {
  StartFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.Create("/r/a").ok());
  ASSERT_TRUE(h.Create("/r/b").ok());
  uint64_t handle = h.AddChunk("/r/a", 0).chunk().handle();
  uint64_t second = h.AddChunk("/r/a", 1).chunk().handle();
  auto lease = h.FindLeaseHolder("/r/a", 0);
  ASSERT_EQ(lease.version(), 2u);
  ASSERT_TRUE(h.Rename("/r/b", "/r/c").ok());
  ASSERT_TRUE(h.Remove("/r/c").ok());

  for (auto& f : fakes_) f->StopHeartbeats();
  h.Restart();
  uint64_t count = 0;
  ASSERT_TRUE(h.Open("/r/a", &count).ok());
  EXPECT_EQ(count, 2u);
  EXPECT_EQ(h.Open("/r/c").error_code(), grpc::StatusCode::NOT_FOUND);
  ASSERT_EQ(h.List("/r", true).size(), 2u);
  auto located = h.FindLocation("/r/a", 0);
  ASSERT_EQ(located.chunks_size(), 1);
  EXPECT_EQ(located.chunks(0).handle(), handle);
  EXPECT_EQ(located.chunks(0).version(), 2u);
  EXPECT_EQ(located.chunks(0).replicas_size(), 0);
  EXPECT_EQ(h.FindLeaseHolder("/r/a", 0).code(), rpc::NO_REPLICAS);

  for (auto& f : fakes_) f->StartHeartbeats(h.Address(), 100);
  ASSERT_TRUE(WaitFor(
      [&] { return h.FindLocation("/r/a", 0).chunks(0).replicas_size() == 3; },
      3000));
  EXPECT_EQ(h.FindLocation("/r/a", 1).chunks(0).handle(), second);
  ASSERT_TRUE(h.Create("/r/new").ok());
  EXPECT_GT(h.AddChunk("/r/new", 0).chunk().handle(), second);
  auto regranted = h.FindLeaseHolder("/r/a", 0);
  ASSERT_EQ(regranted.code(), rpc::OK);
  EXPECT_EQ(regranted.version(), 3u);
}

TEST_F(MasterTest, GarbageCollectionRemovesHiddenFilesAndOrphanChunks) {
  StartFakes(3);
  auto& h = *harness_;
  ASSERT_TRUE(h.Create("/g").ok());
  uint64_t handle = h.AddChunk("/g", 0).chunk().handle();
  for (auto& f : fakes_) ASSERT_TRUE(f->Holds(handle));
  ASSERT_TRUE(h.Remove("/g").ok());
  EXPECT_EQ(h.List("/", true).size(), 1u);
  ASSERT_TRUE(WaitFor([&] { return h.List("/", true).empty(); }, 4000));
  ASSERT_TRUE(WaitFor(
      [&] {
        std::lock_guard<std::mutex> lock(h.master().State().mutex);
        return h.master().State().chunks.Find(handle) == nullptr;
      },
      2000));
  for (auto& f : fakes_)
    EXPECT_TRUE(WaitFor([&] { return !f->Holds(handle); }, 2000));
}

TEST_F(MasterTest, CheckpointRotationSurvivesRestart) {
  harness_.reset();
  Config config = TestConfig(dir_);
  config.checkpoint_log_threshold = 512;
  harness_ = std::make_unique<MasterHarness>(config);
  auto& h = *harness_;
  for (int i = 0; i < 60; ++i)
    ASSERT_TRUE(h.Create("/many/f" + std::to_string(i)).ok());
  ASSERT_TRUE(WaitFor([&] { return !Checkpointer::List(dir_).empty(); }, 3000));
  h.Restart();
  EXPECT_EQ(h.List("/many").size(), 60u);
  ASSERT_TRUE(h.Create("/many/after").ok());
  h.Restart();
  EXPECT_EQ(h.List("/many").size(), 61u);
}

}  // namespace gfs
