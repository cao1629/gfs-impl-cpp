#include "cluster.h"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <random>
#include <thread>

#include "common/crc32.h"

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
  for (const auto& e : entries) out.push_back(e.name);
  std::sort(out.begin(), out.end());
  return out;
}

std::vector<rpc::Replica> ReplicasOf(LocalCluster& cluster,
                                     const std::string& path, uint64_t index) {
  auto stub = cluster.MasterStub();
  grpc::ClientContext ctx;
  rpc::FindLocationRequest req;
  req.set_path(path);
  req.set_first_index(index);
  req.set_count(1);
  rpc::FindLocationResponse resp;
  auto status = stub->FindLocation(&ctx, req, &resp);
  if (!status.ok() || resp.chunks_size() == 0) return {};
  return {resp.chunks(0).replicas().begin(), resp.chunks(0).replicas().end()};
}

bool HasReplicaAt(const std::vector<rpc::Replica>& replicas,
                  const std::string& address) {
  for (const auto& r : replicas) {
    if (r.address() == address) return true;
  }
  return false;
}

}  // namespace

TEST(Cluster, CreateListDeleteAndUndelete) {
  LocalCluster cluster(3);
  auto client = cluster.client();
  ASSERT_TRUE(client->Create("/a/b").ok());
  ASSERT_TRUE(client->Create("/a/c").ok());
  ASSERT_TRUE(client->Create("/a/sub/d").ok());
  EXPECT_EQ(client->Create("/a/b").code, ErrorCode::kAlreadyExists);
  EXPECT_EQ(client->Create("/a/b/under-a-file").code,
            ErrorCode::kInvalidArgument);

  std::vector<DirEntry> entries;
  ASSERT_TRUE(client->List("/a", &entries).ok());
  EXPECT_EQ(Names(entries), (std::vector<std::string>{"b", "c", "sub"}));
  ASSERT_TRUE(client->List("/", &entries).ok());
  EXPECT_EQ(Names(entries), (std::vector<std::string>{"a"}));

  ASSERT_TRUE(client->Remove("/a/b").ok());
  FileInfo info;
  EXPECT_EQ(client->Open("/a/b", &info).code, ErrorCode::kNotFound);
  ASSERT_TRUE(client->List("/a", &entries).ok());
  EXPECT_EQ(Names(entries), (std::vector<std::string>{"c", "sub"}));
  ASSERT_TRUE(client->List("/a", &entries, true).ok());
  ASSERT_EQ(entries.size(), 3u);
  std::string hidden;
  for (const auto& e : entries) {
    if (e.name.rfind(".deleted.", 0) == 0) hidden = e.name;
  }
  ASSERT_FALSE(hidden.empty());
  ASSERT_TRUE(client->Rename("/a/" + hidden, "/a/b").ok());
  EXPECT_TRUE(client->Open("/a/b", &info).ok());
  EXPECT_EQ(client->Remove("/a").code, ErrorCode::kNotFound);
}

TEST(Cluster, WriteAcrossChunkBoundaryReadsBack) {
  LocalCluster cluster(3);
  auto client = cluster.client();
  const uint64_t chunk = 1 << 20;
  std::string data = Pattern(chunk * 2 + chunk / 2, 7);
  ASSERT_TRUE(client->Create("/big").ok());
  ASSERT_TRUE(client->Write("/big", 0, data).ok());

  std::string back;
  ASSERT_TRUE(client->Read("/big", 0, data.size() + 1000, &back).ok());
  EXPECT_EQ(back, data);

  ASSERT_TRUE(client->Read("/big", chunk - 10, 20, &back).ok());
  EXPECT_EQ(back, data.substr(chunk - 10, 20));

  uint64_t length = 0;
  ASSERT_TRUE(client->Length("/big", &length).ok());
  EXPECT_EQ(length, data.size());

  FileInfo info;
  ASSERT_TRUE(client->Open("/big", &info).ok());
  EXPECT_EQ(info.chunk_count, 3u);

  std::string small = "hello";
  ASSERT_TRUE(client->Write("/big", chunk + 100, small).ok());
  ASSERT_TRUE(client->Read("/big", chunk + 98, 9, &back).ok());
  EXPECT_EQ(back,
            data.substr(chunk + 98, 2) + small + data.substr(chunk + 105, 2));
}

TEST(Cluster, ConcurrentRecordAppendsStayIntact) {
  LocalCluster cluster(3);
  const size_t record_size = 5000;
  const int writers = 4;
  const int per_writer = 60;
  std::vector<std::vector<std::pair<uint64_t, uint32_t>>> offsets(writers);
  {
    auto setup = cluster.client();
    ASSERT_TRUE(setup->Create("/log").ok());
  }
  std::vector<std::thread> threads;
  std::atomic<int> failures{0};
  for (int w = 0; w < writers; ++w) {
    threads.emplace_back([&, w] {
      auto client = cluster.client();
      for (int i = 0; i < per_writer; ++i) {
        uint32_t seed = static_cast<uint32_t>(w * 100000 + i);
        uint64_t offset = 0;
        Status st =
            client->RecordAppend("/log", Pattern(record_size, seed), &offset);
        if (!st.ok()) {
          ++failures;
          continue;
        }
        offsets[w].emplace_back(offset, seed);
      }
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_EQ(failures.load(), 0);

  auto reader = cluster.client();
  size_t seen = 0;
  for (int w = 0; w < writers; ++w) {
    for (const auto& [offset, seed] : offsets[w]) {
      std::string back;
      ASSERT_TRUE(reader->Read("/log", offset, record_size, &back).ok());
      EXPECT_EQ(back, Pattern(record_size, seed))
          << "writer " << w << " offset " << offset;
      ++seen;
    }
  }
  EXPECT_EQ(seen, static_cast<size_t>(writers * per_writer));
}

TEST(Cluster, RecordAppendPadsAtChunkBoundary) {
  LocalCluster cluster(3);
  auto client = cluster.client();
  ASSERT_TRUE(client->Create("/pad").ok());
  const size_t record = 250 * 1024;
  std::vector<uint64_t> offsets;
  for (int i = 0; i < 5; ++i) {
    uint64_t offset = 0;
    ASSERT_TRUE(client->RecordAppend("/pad", Pattern(record, i), &offset).ok());
    offsets.push_back(offset);
  }
  EXPECT_EQ(offsets[3], 3 * record);
  EXPECT_EQ(offsets[4], 1u << 20);
  std::string back;
  ASSERT_TRUE(client->Read("/pad", offsets[4], record, &back).ok());
  EXPECT_EQ(back, Pattern(record, 4));
  EXPECT_EQ(client->RecordAppend("/pad", Pattern(300 * 1024, 9), nullptr).code,
            ErrorCode::kInvalidArgument);
}

TEST(Cluster, ChunkserverDeathThenWritesRecover) {
  LocalCluster cluster(3);
  auto client = cluster.client();
  ASSERT_TRUE(client->Create("/f").ok());
  ASSERT_TRUE(client->Write("/f", 0, Pattern(100, 1)).ok());
  auto before = ReplicasOf(cluster, "/f", 0);
  ASSERT_EQ(before.size(), 3u);
  ASSERT_TRUE(HasReplicaAt(before, cluster.ChunkserverAddress(0)));

  cluster.KillChunkserver(0);
  ASSERT_TRUE(client->Write("/f", 100, Pattern(100, 2)).ok());
  ASSERT_TRUE(client->Write("/f", 0, Pattern(50, 3)).ok());

  std::string back;
  ASSERT_TRUE(client->Read("/f", 0, 200, &back).ok());
  EXPECT_EQ(back,
            Pattern(50, 3) + Pattern(100, 1).substr(50) + Pattern(100, 2));

  SleepMs(1500);
  auto after = ReplicasOf(cluster, "/f", 0);
  EXPECT_EQ(after.size(), 2u);
  EXPECT_FALSE(HasReplicaAt(after, cluster.ChunkserverAddress(0)));

  cluster.RestartChunkserver(0);
  SleepMs(1000);
  auto restarted = ReplicasOf(cluster, "/f", 0);
  EXPECT_EQ(restarted.size(), 2u);
  EXPECT_FALSE(HasReplicaAt(restarted, cluster.ChunkserverAddress(0)));

  ASSERT_TRUE(client->Read("/f", 0, 200, &back).ok());
  EXPECT_EQ(back,
            Pattern(50, 3) + Pattern(100, 1).substr(50) + Pattern(100, 2));
}

TEST(Cluster, MasterRestartKeepsMetadata) {
  LocalCluster cluster(3);
  auto client = cluster.client();
  std::string data = Pattern((1 << 20) + 500, 11);
  ASSERT_TRUE(client->Create("/keep/a").ok());
  ASSERT_TRUE(client->Create("/keep/b").ok());
  ASSERT_TRUE(client->Write("/keep/a", 0, data).ok());
  ASSERT_TRUE(client->Rename("/keep/b", "/keep/c").ok());

  cluster.KillMaster();
  cluster.RestartMaster();

  auto fresh = cluster.client();
  std::vector<DirEntry> entries;
  ASSERT_TRUE(fresh->List("/keep", &entries).ok());
  EXPECT_EQ(Names(entries), (std::vector<std::string>{"a", "c"}));
  std::string back;
  ASSERT_TRUE(fresh->Read("/keep/a", 0, data.size(), &back).ok());
  EXPECT_EQ(back, data);
  ASSERT_TRUE(fresh->Write("/keep/a", 10, "after-restart").ok());
  ASSERT_TRUE(fresh->Read("/keep/a", 10, 13, &back).ok());
  EXPECT_EQ(back, "after-restart");
}

TEST(Cluster, CheckpointIsWrittenAndLoaded) {
  LocalCluster cluster(3, {{"checkpoint_log_threshold", "1K"}});
  auto client = cluster.client();
  for (int i = 0; i < 80; ++i) {
    ASSERT_TRUE(client->Create("/many/file" + std::to_string(i)).ok());
  }
  ASSERT_TRUE(client->Write("/many/file3", 0, "payload").ok());
  SleepMs(500);
  bool checkpoint_seen = false;
  for (const auto& entry :
       std::filesystem::directory_iterator(cluster.MasterDataDir())) {
    if (entry.path().filename().string().rfind("checkpoint.", 0) == 0 &&
        entry.path().extension() != ".tmp")
      checkpoint_seen = true;
  }
  EXPECT_TRUE(checkpoint_seen);

  cluster.KillMaster();
  cluster.RestartMaster();
  auto fresh = cluster.client();
  std::vector<DirEntry> entries;
  ASSERT_TRUE(fresh->List("/many", &entries).ok());
  EXPECT_EQ(entries.size(), 80u);
  std::string back;
  ASSERT_TRUE(fresh->Read("/many/file3", 0, 7, &back).ok());
  EXPECT_EQ(back, "payload");
}

TEST(Cluster, SnapshotIsolatesWrites) {
  LocalCluster cluster(3);
  auto client = cluster.client();
  ASSERT_TRUE(client->Create("/src").ok());
  ASSERT_TRUE(client->Write("/src", 0, "AAAA").ok());
  ASSERT_TRUE(client->Snapshot("/src", "/snap").ok());

  ASSERT_TRUE(client->Write("/src", 0, "BBBB").ok());
  std::string back;
  ASSERT_TRUE(client->Read("/snap", 0, 4, &back).ok());
  EXPECT_EQ(back, "AAAA");
  ASSERT_TRUE(client->Read("/src", 0, 4, &back).ok());
  EXPECT_EQ(back, "BBBB");

  ASSERT_TRUE(client->Write("/snap", 0, "CCCC").ok());
  ASSERT_TRUE(client->Read("/src", 0, 4, &back).ok());
  EXPECT_EQ(back, "BBBB");
  ASSERT_TRUE(client->Read("/snap", 0, 4, &back).ok());
  EXPECT_EQ(back, "CCCC");

  ASSERT_TRUE(client->Create("/d/x").ok());
  ASSERT_TRUE(client->Create("/d/y/z").ok());
  ASSERT_TRUE(client->Write("/d/x", 0, "xx").ok());
  ASSERT_TRUE(client->Snapshot("/d", "/e").ok());
  std::vector<DirEntry> entries;
  ASSERT_TRUE(client->List("/e", &entries).ok());
  EXPECT_EQ(Names(entries), (std::vector<std::string>{"x", "y"}));
  ASSERT_TRUE(client->Read("/e/x", 0, 2, &back).ok());
  EXPECT_EQ(back, "xx");
  EXPECT_EQ(client->Snapshot("/d", "/e").code, ErrorCode::kAlreadyExists);
}

TEST(Cluster, DeleteThenGarbageCollectionRemovesChunkFiles) {
  LocalCluster cluster(3);
  auto client = cluster.client();
  ASSERT_TRUE(client->Create("/gone").ok());
  ASSERT_TRUE(client->Write("/gone", 0, Pattern(4096, 5)).ok());
  EXPECT_EQ(cluster.ChunkFilesOnDisk(), 3u);
  ASSERT_TRUE(client->Remove("/gone").ok());
  SleepMs(4500);
  EXPECT_EQ(cluster.ChunkFilesOnDisk(), 0u);
}

TEST(Cluster, RenameFileAndDirectory) {
  LocalCluster cluster(3);
  auto client = cluster.client();
  ASSERT_TRUE(client->Create("/a/b").ok());
  ASSERT_TRUE(client->Write("/a/b", 0, "data").ok());
  ASSERT_TRUE(client->Rename("/a/b", "/a/c").ok());
  ASSERT_TRUE(client->Rename("/a", "/z").ok());
  std::string back;
  ASSERT_TRUE(client->Read("/z/c", 0, 4, &back).ok());
  EXPECT_EQ(back, "data");
  FileInfo info;
  EXPECT_EQ(client->Open("/a/b", &info).code, ErrorCode::kNotFound);
  EXPECT_EQ(client->Rename("/z/c", "/z/c").code, ErrorCode::kAlreadyExists);
}

}  // namespace gfs::testing
