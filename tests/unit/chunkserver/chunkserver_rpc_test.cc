#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "chunkserver/chunk_store.h"
#include "chunkserver/chunkserver.h"
#include "chunkserver/chunkserver_service.h"
#include "common/ids.h"
#include "gfs.grpc.pb.h"

namespace gfs {

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kChunk = 1 << 20;

Config testConfig() {
  Config config;
  config.chunk_size = kChunk;
  config.checksum_block_size = 64 << 10;
  config.lease_duration = Millis(1500);
  config.lease_clock_skew_margin = Millis(100);
  config.heartbeat_interval = Millis(60000);
  config.rpc_deadline = Millis(2000);
  config.client_rpc_deadline = Millis(5000);
  config.data_buffer_capacity = 8 << 20;
  config.master_address = "127.0.0.1:1";
  return config;
}

struct TestServer {
  std::string dir;
  std::string id;
  std::string address;
  std::unique_ptr<ChunkStore> store;
  std::unique_ptr<Chunkserver> chunkserver;
  std::unique_ptr<ChunkserverService> service;
  std::unique_ptr<grpc::Server> server;
  std::unique_ptr<rpc::Chunkserver::Stub> stub;

  explicit TestServer(const std::string& rack) {
    dir = (fs::temp_directory_path() / ("gfs-cs-" + randomHexId(6))).string();
    fs::create_directories(dir);
    Config config = testConfig();
    config.data_dir = dir;
    config.rack = rack;
    store = std::make_unique<ChunkStore>(dir, config.chunk_size, config.checksum_block_size);
    store->scan();
    id = loadOrCreateChunkserverId(dir);
    chunkserver = std::make_unique<Chunkserver>(config, id, store.get());
    service = std::make_unique<ChunkserverService>(chunkserver.get());
    int port = 0;
    server = startChunkserverServer(service.get(), "127.0.0.1:0", config.chunk_size, &port);
    address = "127.0.0.1:" + std::to_string(port);
    chunkserver->setAdvertiseAddress(address);
    stub = rpc::Chunkserver::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
  }

  ~TestServer() {
    server->Shutdown();
    chunkserver->stop();
    fs::remove_all(dir);
  }

  rpc::Replica replica() const {
    rpc::Replica r;
    r.set_chunkserver_id(id);
    r.set_address(address);
    return r;
  }
};

std::string pattern(size_t n, char base) {
  std::string s(n, '\0');
  for (size_t i = 0; i < n; ++i) s[i] = static_cast<char>(base + (i % 19));
  return s;
}

class ChunkserverRpcTest : public ::testing::Test {
 protected:
  void SetUp() override {
    for (int i = 0; i < 3; ++i) servers_.push_back(std::make_unique<TestServer>("rack" + std::to_string(i % 2)));
    for (auto& s : servers_) ASSERT_EQ(createChunk(*s, 1, 1, 0), rpc::OK);
    grant(1, 2);
  }

  rpc::ResultCode createChunk(TestServer& s, uint64_t handle, uint64_t version, uint64_t copy_from) {
    grpc::ClientContext ctx;
    rpc::CreateChunkRequest req;
    req.set_handle(handle);
    req.set_version(version);
    req.set_copy_from(copy_from);
    rpc::CreateChunkResponse resp;
    EXPECT_TRUE(s.stub->CreateChunk(&ctx, req, &resp).ok());
    return resp.code();
  }

  void grant(uint64_t handle, uint64_t version, uint64_t lease_ms = 1500) {
    {
      grpc::ClientContext ctx;
      rpc::GrantLeaseRequest req;
      req.set_handle(handle);
      req.set_version(version);
      req.set_lease_ms(lease_ms);
      *req.add_secondaries() = servers_[1]->replica();
      *req.add_secondaries() = servers_[2]->replica();
      rpc::GrantLeaseResponse resp;
      ASSERT_TRUE(servers_[0]->stub->GrantLease(&ctx, req, &resp).ok());
      ASSERT_EQ(resp.code(), rpc::OK);
    }
    for (int i = 1; i < 3; ++i) {
      grpc::ClientContext ctx;
      rpc::UpdateVersionRequest req;
      req.set_handle(handle);
      req.set_version(version);
      rpc::UpdateVersionResponse resp;
      ASSERT_TRUE(servers_[i]->stub->UpdateVersion(&ctx, req, &resp).ok());
      ASSERT_EQ(resp.code(), rpc::OK);
    }
  }

  rpc::PushDataResponse push(const std::string& data, uint64_t sequence, std::vector<int> chain = {0, 1, 2}) {
    grpc::ClientContext ctx;
    rpc::PushDataResponse resp;
    auto writer = servers_[chain[0]]->stub->PushData(&ctx, &resp);
    rpc::PushDataRequest header;
    header.mutable_header()->set_client_id("client-a");
    header.mutable_header()->set_sequence(sequence);
    header.mutable_header()->set_total_length(data.size());
    for (size_t i = 1; i < chain.size(); ++i) *header.mutable_header()->add_forward_to() = servers_[chain[i]]->replica();
    EXPECT_TRUE(writer->Write(header));
    const size_t frame = 30000;
    for (size_t pos = 0; pos < data.size(); pos += frame) {
      rpc::PushDataRequest msg;
      msg.set_data(data.substr(pos, frame));
      EXPECT_TRUE(writer->Write(msg));
    }
    writer->WritesDone();
    EXPECT_TRUE(writer->Finish().ok());
    return resp;
  }

  rpc::WriteResponse write(int server, uint64_t handle, uint64_t version, uint64_t offset, uint64_t sequence) {
    grpc::ClientContext ctx;
    rpc::WriteRequest req;
    req.set_handle(handle);
    req.set_version(version);
    req.set_offset(offset);
    req.set_client_id("client-a");
    req.set_sequence(sequence);
    rpc::WriteResponse resp;
    EXPECT_TRUE(servers_[server]->stub->Write(&ctx, req, &resp).ok());
    return resp;
  }

  rpc::RecordAppendResponse append(uint64_t handle, uint64_t version, uint64_t sequence) {
    grpc::ClientContext ctx;
    rpc::RecordAppendRequest req;
    req.set_handle(handle);
    req.set_version(version);
    req.set_client_id("client-a");
    req.set_sequence(sequence);
    rpc::RecordAppendResponse resp;
    EXPECT_TRUE(servers_[0]->stub->RecordAppend(&ctx, req, &resp).ok());
    return resp;
  }

  rpc::ReadResponse read(int server, uint64_t handle, uint64_t version, uint64_t offset, uint64_t length) {
    grpc::ClientContext ctx;
    rpc::ReadRequest req;
    req.set_handle(handle);
    req.set_version(version);
    req.set_offset(offset);
    req.set_length(length);
    rpc::ReadResponse resp;
    EXPECT_TRUE(servers_[server]->stub->Read(&ctx, req, &resp).ok());
    return resp;
  }

  uint64_t length(int server, uint64_t handle) {
    grpc::ClientContext ctx;
    rpc::GetChunkLengthRequest req;
    req.set_handle(handle);
    rpc::GetChunkLengthResponse resp;
    EXPECT_TRUE(servers_[server]->stub->GetChunkLength(&ctx, req, &resp).ok());
    EXPECT_EQ(resp.code(), rpc::OK);
    return resp.length();
  }

  std::vector<std::unique_ptr<TestServer>> servers_;
};

}

TEST_F(ChunkserverRpcTest, PushAlongChainThenWriteReachesEveryReplica) {
  std::string data = pattern(100 * 1024, 'a');
  auto pushed = push(data, 1);
  ASSERT_EQ(pushed.code(), rpc::OK);
  auto written = write(0, 1, 2, 0, 1);
  ASSERT_EQ(written.code(), rpc::OK) << written.failed_at();
  for (int i = 0; i < 3; ++i) {
    auto got = read(i, 1, 2, 0, data.size());
    ASSERT_EQ(got.code(), rpc::OK) << "replica " << i;
    EXPECT_EQ(got.data(), data) << "replica " << i;
    EXPECT_EQ(length(i, 1), data.size());
  }
  auto older = read(1, 1, 1, 10, 5);
  EXPECT_EQ(older.code(), rpc::OK);
  EXPECT_EQ(older.data(), data.substr(10, 5));
}

TEST_F(ChunkserverRpcTest, RecordAppendAssignsOffsetsAndPadsAtTheBoundary) {
  std::string first = pattern(100 * 1024, 'b');
  ASSERT_EQ(push(first, 10).code(), rpc::OK);
  auto a1 = append(1, 2, 10);
  ASSERT_EQ(a1.code(), rpc::OK) << a1.failed_at();
  EXPECT_EQ(a1.offset(), 0u);

  std::string second = pattern(200 * 1024, 'c');
  ASSERT_EQ(push(second, 11).code(), rpc::OK);
  auto a2 = append(1, 2, 11);
  ASSERT_EQ(a2.code(), rpc::OK) << a2.failed_at();
  EXPECT_EQ(a2.offset(), first.size());

  std::string big = pattern(900 * 1024, 'd');
  ASSERT_EQ(push(big, 12).code(), rpc::OK);
  auto a3 = append(1, 2, 12);
  EXPECT_EQ(a3.code(), rpc::RETRY_NEXT_CHUNK);
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(length(i, 1), kChunk) << "replica " << i;
    auto got = read(i, 1, 2, first.size(), second.size());
    ASSERT_EQ(got.code(), rpc::OK);
    EXPECT_EQ(got.data(), second);
    auto tail = read(i, 1, 2, kChunk - 8, 8);
    ASSERT_EQ(tail.code(), rpc::OK);
    EXPECT_EQ(tail.data(), std::string(8, '\0'));
  }
  ASSERT_EQ(push("tiny", 13).code(), rpc::OK);
  EXPECT_EQ(append(1, 2, 13).code(), rpc::RETRY_NEXT_CHUNK);
}

TEST_F(ChunkserverRpcTest, RejectsMissingDataWrongVersionAndNonPrimary) {
  EXPECT_EQ(write(0, 1, 2, 0, 999).code(), rpc::DATA_MISSING);
  ASSERT_EQ(push("data", 20).code(), rpc::OK);
  EXPECT_EQ(write(0, 1, 1, 0, 20).code(), rpc::STALE_VERSION);
  EXPECT_EQ(write(1, 1, 2, 0, 20).code(), rpc::NOT_PRIMARY);
  EXPECT_EQ(write(0, 42, 2, 0, 20).code(), rpc::NO_SUCH_CHUNK);
  EXPECT_EQ(write(0, 1, 2, kChunk - 2, 20).code(), rpc::OUT_OF_RANGE);
  EXPECT_EQ(write(0, 1, 2, 0, 20).code(), rpc::OK);
  EXPECT_EQ(read(2, 1, 5, 0, 4).code(), rpc::STALE_VERSION);
  EXPECT_EQ(read(2, 7, 2, 0, 4).code(), rpc::NO_SUCH_CHUNK);
}

TEST_F(ChunkserverRpcTest, SecondaryWithNewerVersionFencesTheOldPrimary) {
  {
    grpc::ClientContext ctx;
    rpc::UpdateVersionRequest req;
    req.set_handle(1);
    req.set_version(3);
    rpc::UpdateVersionResponse resp;
    ASSERT_TRUE(servers_[1]->stub->UpdateVersion(&ctx, req, &resp).ok());
  }
  ASSERT_EQ(push("fenced", 30).code(), rpc::OK);
  auto written = write(0, 1, 2, 0, 30);
  EXPECT_EQ(written.code(), rpc::FAILED);
  EXPECT_EQ(written.failed_at(), servers_[1]->id);
  auto primary_copy = read(0, 1, 2, 0, 6);
  EXPECT_EQ(primary_copy.data(), "fenced");
  auto untouched = read(1, 1, 3, 0, 6);
  EXPECT_EQ(untouched.code(), rpc::OK);
  EXPECT_TRUE(untouched.data().empty());
}

TEST_F(ChunkserverRpcTest, LeaseExpiryRevokeAndRegrant) {
  ASSERT_EQ(push("one", 40).code(), rpc::OK);
  EXPECT_EQ(write(0, 1, 2, 0, 40).code(), rpc::OK);
  std::this_thread::sleep_for(std::chrono::milliseconds(1600));
  ASSERT_EQ(push("two", 41).code(), rpc::OK);
  EXPECT_EQ(write(0, 1, 2, 0, 41).code(), rpc::LEASE_EXPIRED);

  grant(1, 3);
  ASSERT_EQ(push("three", 42).code(), rpc::OK);
  EXPECT_EQ(write(0, 1, 3, 0, 42).code(), rpc::OK);
  {
    grpc::ClientContext ctx;
    rpc::RevokeLeaseRequest req;
    req.set_handle(1);
    rpc::RevokeLeaseResponse resp;
    ASSERT_TRUE(servers_[0]->stub->RevokeLease(&ctx, req, &resp).ok());
  }
  ASSERT_EQ(push("four", 43).code(), rpc::OK);
  EXPECT_EQ(write(0, 1, 3, 0, 43).code(), rpc::NOT_PRIMARY);
  EXPECT_EQ(read(2, 1, 3, 0, 5).data(), "three");
}

TEST_F(ChunkserverRpcTest, CorruptReplicaReportsChecksumMismatch) {
  std::string data = pattern(70000, 'k');
  ASSERT_EQ(push(data, 50).code(), rpc::OK);
  ASSERT_EQ(write(0, 1, 2, 0, 50).code(), rpc::OK);
  std::string path = servers_[1]->store->chunkPath(1);
  {
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(100);
    f.write("X", 1);
  }
  EXPECT_EQ(read(1, 1, 2, 0, 200).code(), rpc::CHECKSUM_MISMATCH);
  EXPECT_EQ(read(1, 1, 2, 65536, 100).code(), rpc::OK);
  EXPECT_EQ(read(0, 1, 2, 0, 200).code(), rpc::OK);
  EXPECT_EQ(servers_[1]->store->corruptHandles(), std::vector<uint64_t>{1});
}

TEST_F(ChunkserverRpcTest, PushChainBreaksAtADeadHop) {
  rpc::Replica dead;
  dead.set_chunkserver_id("dead-server");
  dead.set_address("127.0.0.1:1");
  grpc::ClientContext ctx;
  rpc::PushDataResponse resp;
  auto writer = servers_[0]->stub->PushData(&ctx, &resp);
  rpc::PushDataRequest header;
  header.mutable_header()->set_client_id("client-a");
  header.mutable_header()->set_sequence(60);
  header.mutable_header()->set_total_length(4);
  *header.mutable_header()->add_forward_to() = servers_[1]->replica();
  *header.mutable_header()->add_forward_to() = dead;
  ASSERT_TRUE(writer->Write(header));
  rpc::PushDataRequest msg;
  msg.set_data("data");
  writer->Write(msg);
  writer->WritesDone();
  ASSERT_TRUE(writer->Finish().ok());
  EXPECT_EQ(resp.code(), rpc::FAILED);
  EXPECT_EQ(resp.failed_at(), "dead-server");
}

TEST_F(ChunkserverRpcTest, CreateChunkIsIdempotentAndCopiesLocally) {
  EXPECT_EQ(createChunk(*servers_[0], 1, 2, 0), rpc::OK);
  EXPECT_EQ(createChunk(*servers_[0], 1, 9, 0), rpc::FAILED);
  ASSERT_EQ(push("copied", 70).code(), rpc::OK);
  ASSERT_EQ(write(0, 1, 2, 0, 70).code(), rpc::OK);
  EXPECT_EQ(createChunk(*servers_[0], 2, 1, 1), rpc::OK);
  EXPECT_EQ(read(0, 2, 1, 0, 6).data(), "copied");
  EXPECT_EQ(createChunk(*servers_[0], 3, 1, 77), rpc::NO_SUCH_CHUNK);
}

}
