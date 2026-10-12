#include <gtest/gtest.h>

#include "client/gfs_client.h"
#include "fake_cluster.h"

namespace gfs::testing {

namespace {

std::string Pattern(size_t size, uint32_t seed) {
  std::string out(size, '\0');
  uint32_t x = seed * 2654435761u + 12345u;
  for (size_t i = 0; i < size; ++i) {
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    out[i] = static_cast<char>('a' + (x % 26));
  }
  return out;
}

std::vector<std::string> Names(const std::vector<DirEntry>& entries) {
  std::vector<std::string> out;
  for (const auto& e : entries)
    out.push_back(e.name + (e.is_directory ? "/" : ""));
  return out;
}

}  // namespace

TEST(ClientLibrary, ConstructorDoesNotContactMaster) {
  Config config;
  config.master_address = "127.0.0.1:1";
  config.client_rpc_deadline = Millis(300);
  Client client(config);
  EXPECT_EQ(client.ClientId().size(), 32u);
  EXPECT_EQ(client.ChunkSize(), 0u);
  EXPECT_EQ(client.Create("/x").code, ErrorCode::kUnavailable);
}

TEST(ClientLibrary, WriteAcrossChunkBoundaryReadsBack) {
  FakeCluster cluster;
  Client client(cluster.ClientConfig());
  EXPECT_EQ(client.ChunkSize(), kFakeChunkSize);
  std::string data = Pattern(kFakeChunkSize * 2 + 500, 3);
  ASSERT_TRUE(client.Create("/big").ok());
  ASSERT_TRUE(client.Write("/big", 0, data).ok());

  FileInfo info;
  ASSERT_TRUE(client.Open("/big", &info).ok());
  EXPECT_EQ(info.chunk_count, 3u);

  std::string back;
  ASSERT_TRUE(client.Read("/big", 0, data.size() + 100, &back).ok());
  EXPECT_EQ(back, data);

  ASSERT_TRUE(client.Read("/big", kFakeChunkSize - 7, 15, &back).ok());
  EXPECT_EQ(back, data.substr(kFakeChunkSize - 7, 15));

  ASSERT_TRUE(client.Write("/big", kFakeChunkSize + 10, "hello").ok());
  ASSERT_TRUE(client.Read("/big", kFakeChunkSize + 8, 9, &back).ok());
  EXPECT_EQ(back, data.substr(kFakeChunkSize + 8, 2) + "hello" +
                      data.substr(kFakeChunkSize + 15, 2));

  for (auto& server : cluster.servers) {
    EXPECT_EQ(server->ChunkData(1), data.substr(0, kFakeChunkSize));
  }
}

TEST(ClientLibrary, WriteBeyondCurrentChunksCreatesThemInOrder) {
  FakeCluster cluster;
  Client client(cluster.ClientConfig());
  ASSERT_TRUE(client.Create("/sparse").ok());
  ASSERT_TRUE(client.Write("/sparse", kFakeChunkSize * 2 + 100, "tail").ok());
  FileInfo info;
  ASSERT_TRUE(client.Open("/sparse", &info).ok());
  EXPECT_EQ(info.chunk_count, 3u);
  std::string back;
  ASSERT_TRUE(client.Read("/sparse", kFakeChunkSize * 2 + 100, 4, &back).ok());
  EXPECT_EQ(back, "tail");
  EXPECT_TRUE(client.Write("/sparse", 5, "").ok());
}

TEST(ClientLibrary, RecordAppendOffsetsIncreaseAndPadAtBoundary) {
  FakeCluster cluster;
  Client client(cluster.ClientConfig());
  ASSERT_TRUE(client.Create("/log").ok());
  const size_t record = 15 * 1024;
  std::vector<uint64_t> offsets;
  for (int i = 0; i < 6; ++i) {
    uint64_t offset = 0;
    ASSERT_TRUE(client.RecordAppend("/log", Pattern(record, i), &offset).ok())
        << i;
    offsets.push_back(offset);
  }
  for (int i = 0; i < 4; ++i) EXPECT_EQ(offsets[i], record * i);
  EXPECT_EQ(offsets[4], kFakeChunkSize);
  EXPECT_EQ(offsets[5], kFakeChunkSize + record);
  for (int i = 0; i < 6; ++i) {
    std::string back;
    ASSERT_TRUE(client.Read("/log", offsets[i], record, &back).ok());
    EXPECT_EQ(back, Pattern(record, i)) << i;
  }
  uint64_t length = 0;
  ASSERT_TRUE(client.Length("/log", &length).ok());
  EXPECT_EQ(length, kFakeChunkSize + 2 * record);
  ASSERT_TRUE(client.RecordAppend("/log", "", nullptr).ok());
}

TEST(ClientLibrary, RecordAppendOverLimitIsRejected) {
  FakeCluster cluster;
  Client client(cluster.ClientConfig());
  ASSERT_TRUE(client.Create("/log").ok());
  uint64_t offset = 0;
  EXPECT_EQ(
      client.RecordAppend("/log", Pattern(kFakeMaxAppend + 1, 1), &offset).code,
      ErrorCode::kInvalidArgument);
  EXPECT_EQ(client.RecordAppend("/missing", "x", &offset).code,
            ErrorCode::kNotFound);
}

TEST(ClientLibrary, ReadPastEndIsShort) {
  FakeCluster cluster;
  Client client(cluster.ClientConfig());
  ASSERT_TRUE(client.Create("/short").ok());
  ASSERT_TRUE(client.Write("/short", 0, Pattern(100, 9)).ok());
  std::string back;
  ASSERT_TRUE(client.Read("/short", 50, 1000, &back).ok());
  EXPECT_EQ(back, Pattern(100, 9).substr(50));
  ASSERT_TRUE(client.Read("/short", 200, 10, &back).ok());
  EXPECT_TRUE(back.empty());
  ASSERT_TRUE(client.Read("/short", kFakeChunkSize * 5, 10, &back).ok());
  EXPECT_TRUE(back.empty());
  ASSERT_TRUE(client.Read("/short", 0, 0, &back).ok());
  EXPECT_TRUE(back.empty());
  ASSERT_TRUE(client.Create("/empty").ok());
  ASSERT_TRUE(client.Read("/empty", 0, 10, &back).ok());
  EXPECT_TRUE(back.empty());
  uint64_t length = 1;
  ASSERT_TRUE(client.Length("/empty", &length).ok());
  EXPECT_EQ(length, 0u);
  ASSERT_TRUE(client.Length("/short", &length).ok());
  EXPECT_EQ(length, 100u);
}

TEST(ClientLibrary, StaleReplicaFallsOverToAnother) {
  FakeCluster cluster;
  Client client(cluster.ClientConfig());
  ASSERT_TRUE(client.Create("/f").ok());
  ASSERT_TRUE(client.Write("/f", 0, "payload").ok());
  cluster.servers[0]->stale_reads = 1000;
  cluster.servers[1]->stale_reads = 1000;
  std::string back;
  ASSERT_TRUE(client.Read("/f", 0, 7, &back).ok());
  EXPECT_EQ(back, "payload");
  cluster.servers[2]->stale_reads = 1000;
  EXPECT_EQ(client.Read("/f", 0, 7, &back).code, ErrorCode::kUnavailable);
  for (auto& s : cluster.servers) s->stale_reads = 0;
  ASSERT_TRUE(client.Read("/f", 0, 7, &back).ok());
  EXPECT_EQ(back, "payload");
}

TEST(ClientLibrary, DataMissingTriggersRepush) {
  FakeCluster cluster;
  Client client(cluster.ClientConfig());
  ASSERT_TRUE(client.Create("/f").ok());
  cluster.servers[0]->drop_data = 1;
  ASSERT_TRUE(client.Write("/f", 0, "first").ok());
  EXPECT_EQ(cluster.servers[0]->drop_data.load(), 0);
  EXPECT_GE(cluster.servers[0]->pushes.load(), 2);
  cluster.servers[0]->drop_data = 1;
  uint64_t offset = 0;
  ASSERT_TRUE(client.RecordAppend("/f", "second", &offset).ok());
  EXPECT_EQ(offset, 5u);
  std::string back;
  ASSERT_TRUE(client.Read("/f", 0, 11, &back).ok());
  EXPECT_EQ(back, "firstsecond");
}

TEST(ClientLibrary, LeaseHolderIsCachedAcrossWrites) {
  FakeCluster cluster;
  Client client(cluster.ClientConfig());
  ASSERT_TRUE(client.Create("/f").ok());
  ASSERT_TRUE(client.Write("/f", 0, "a").ok());
  ASSERT_TRUE(client.Write("/f", 1, "b").ok());
  ASSERT_TRUE(client.Write("/f", 2, "c").ok());
  EXPECT_EQ(cluster.master->lease_requests.load(), 1);
}

TEST(ClientLibrary, StatusMapping) {
  FakeCluster cluster;
  Client client(cluster.ClientConfig());
  FileInfo info;
  EXPECT_EQ(client.Open("/nope", &info).code, ErrorCode::kNotFound);
  std::string back;
  EXPECT_EQ(client.Read("/nope", 0, 1, &back).code, ErrorCode::kNotFound);
  EXPECT_EQ(client.Write("/nope", 0, "x").code, ErrorCode::kNotFound);
  EXPECT_EQ(client.Remove("/nope").code, ErrorCode::kNotFound);
  EXPECT_EQ(client.Create("relative").code, ErrorCode::kInvalidArgument);
  ASSERT_TRUE(client.Create("/a").ok());
  EXPECT_EQ(client.Create("/a").code, ErrorCode::kAlreadyExists);
  EXPECT_EQ(client.Create("/a/b").code, ErrorCode::kInvalidArgument);
}

TEST(ClientLibrary, SnapshotWaitsOutTheMastersLeaseWait) {
  FakeCluster cluster;
  Config config = cluster.ClientConfig();
  config.client_rpc_deadline = Millis(300);
  Client client(config);
  ASSERT_TRUE(client.Create("/a/b").ok());
  cluster.master->SetSnapshotDelay(Millis(800));
  EXPECT_TRUE(client.Snapshot("/a", "/s").ok());
}

TEST(ClientLibrary, ListRenameSnapshotRoundTrip) {
  FakeCluster cluster;
  Client client(cluster.ClientConfig());
  ASSERT_TRUE(client.Create("/a/b").ok());
  ASSERT_TRUE(client.Create("/a/c").ok());
  ASSERT_TRUE(client.Create("/a/d/e").ok());
  ASSERT_TRUE(client.Write("/a/b", 0, "bee").ok());
  std::vector<DirEntry> entries;
  ASSERT_TRUE(client.List("/a", &entries).ok());
  EXPECT_EQ(Names(entries), (std::vector<std::string>{"b", "c", "d/"}));

  ASSERT_TRUE(client.Rename("/a/b", "/a/bb").ok());
  ASSERT_TRUE(client.Snapshot("/a", "/s").ok());
  ASSERT_TRUE(client.List("/s", &entries).ok());
  EXPECT_EQ(Names(entries), (std::vector<std::string>{"bb", "c", "d/"}));
  std::string back;
  ASSERT_TRUE(client.Read("/s/bb", 0, 3, &back).ok());
  EXPECT_EQ(back, "bee");
  EXPECT_EQ(client.Snapshot("/a", "/s").code, ErrorCode::kAlreadyExists);
  EXPECT_EQ(client.Rename("/zzz", "/y").code, ErrorCode::kNotFound);

  ASSERT_TRUE(client.Remove("/a/bb").ok());
  ASSERT_TRUE(client.List("/a", &entries).ok());
  EXPECT_EQ(Names(entries), (std::vector<std::string>{"c", "d/"}));
  ASSERT_TRUE(client.List("/", &entries).ok());
  EXPECT_EQ(Names(entries), (std::vector<std::string>{"a/", "s/"}));
}

}  // namespace gfs::testing
