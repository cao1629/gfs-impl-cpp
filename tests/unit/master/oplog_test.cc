#include "master/oplog.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <thread>

#include "master/checkpoint.h"
#include "master/master_state.h"

namespace gfs {

namespace fs = std::filesystem;

namespace {

std::string FreshDir(const std::string& name) {
  std::string dir = (fs::temp_directory_path() /
                     ("gfs-oplog-" + name + "-" + std::to_string(::getpid())))
                        .string();
  fs::remove_all(dir);
  fs::create_directories(dir);
  return dir;
}

state::LogRecord CreateRecord(const std::string& path) {
  state::LogRecord record;
  record.mutable_create()->set_path(path);
  return record;
}

}  // namespace

TEST(OpLog, AppendsFlushesAndReplays) {
  std::string dir = FreshDir("replay");
  Config config;
  config.log_flush_max_delay = Millis(2);
  std::mutex state_mutex;
  {
    OpLog log(config, state_mutex);
    log.AddSink(std::make_unique<LocalFileSink>(dir));
    log.Open(1);
    uint64_t seq = 0;
    for (int i = 0; i < 5; ++i)
      seq = log.Append(CreateRecord("/f" + std::to_string(i)));
    log.WaitFlushed(seq);
  }
  ReplayedSegment segment = OpLog::ReadSegment(dir, 1);
  ASSERT_EQ(segment.records.size(), 5u);
  EXPECT_EQ(segment.records[4].create().path(), "/f4");
  EXPECT_FALSE(segment.torn_tail);

  std::ofstream(OpLog::SegmentPath(dir, 1), std::ios::binary | std::ios::app)
      << "\x09\x00\x00\x00junk";
  ReplayedSegment torn = OpLog::ReadSegment(dir, 1);
  EXPECT_EQ(torn.records.size(), 5u);
  EXPECT_TRUE(torn.torn_tail);
  OpLog::TruncateSegment(dir, 1, torn.valid_bytes);
  EXPECT_FALSE(OpLog::ReadSegment(dir, 1).torn_tail);
  EXPECT_EQ(OpLog::ListSegments(dir), std::vector<uint64_t>{1});
}

TEST(OpLog, RotatesIntoCheckpointsThatRecoveryLoads) {
  std::string dir = FreshDir("rotate");
  Config config;
  config.log_flush_max_delay = Millis(2);
  config.checkpoint_log_threshold = 200;
  MasterState state;
  {
    OpLog log(config, state.mutex);
    log.AddSink(std::make_unique<LocalFileSink>(dir));
    log.EnableCheckpoints(dir, [&] { return state.ToCheckpoint(); });
    log.Open(1);
    for (int i = 0; i < 40; ++i) {
      uint64_t seq = 0;
      {
        std::lock_guard<std::mutex> lock(state.mutex);
        std::string path = "/file" + std::to_string(i);
        state.ApplyCreate(path);
        seq = log.Append(CreateRecord(path));
      }
      log.WaitFlushed(seq);
    }
    EXPECT_GT(log.Segment(), 1u);
  }
  auto checkpoints = Checkpointer::List(dir);
  ASSERT_FALSE(checkpoints.empty());
  EXPECT_LE(checkpoints.size(), 2u);
  std::ofstream(Checkpointer::Path(dir, 999) + ".tmp") << "half written";
  state::Checkpoint loaded;
  uint64_t number = Checkpointer::LoadLatest(dir, &loaded);
  EXPECT_EQ(number, checkpoints.back());
  MasterState recovered;
  recovered.Load(loaded);
  for (uint64_t segment : OpLog::ListSegments(dir)) {
    EXPECT_GE(segment, number);
    for (const auto& record : OpLog::ReadSegment(dir, segment).records)
      recovered.Apply(record);
  }
  EXPECT_EQ(recovered.files.Size(), 40u);
  EXPECT_TRUE(recovered.files.Exists("/file39"));
}

TEST(Checkpointer, IgnoresDamagedFiles) {
  std::string dir = FreshDir("damaged");
  state::Checkpoint good;
  good.set_next_chunk_handle(77);
  good.add_files()->set_path("/keep");
  ASSERT_TRUE(Checkpointer::Write(dir, 3, good));
  std::ofstream(Checkpointer::Path(dir, 4), std::ios::binary)
      << "not a checkpoint";
  state::Checkpoint loaded;
  EXPECT_EQ(Checkpointer::LoadLatest(dir, &loaded), 3u);
  EXPECT_EQ(loaded.next_chunk_handle(), 77u);
  EXPECT_EQ(loaded.files(0).path(), "/keep");
}

TEST(MasterState, ApplyIsIdempotentAndRefcountsFollowSnapshots) {
  MasterState state;
  state.ApplyCreate("/a");
  state.ApplyCreate("/a");
  state.ApplyAddChunk("/a", 0, 10);
  state.ApplyAddChunk("/a", 0, 10);
  state.ApplyAddChunk("/a", 1, 11);
  EXPECT_EQ(state.files.Find("/a")->chunks, (std::vector<uint64_t>{10, 11}));
  EXPECT_EQ(state.next_handle, 12u);
  state.ApplySnapshot("/a", "/b");
  state.ApplySnapshot("/a", "/b");
  EXPECT_EQ(state.chunks.Find(10)->refcount, 2u);
  state.ApplyReplaceChunk("/b", 0, 20);
  EXPECT_EQ(state.chunks.Find(10)->refcount, 1u);
  EXPECT_EQ(state.chunks.Find(20)->refcount, 1u);
  state.ApplyBumpVersion(10, 5);
  state.ApplyBumpVersion(10, 3);
  EXPECT_EQ(state.chunks.Find(10)->version, 5u);
  state.ApplyRemove("/a");
  state.ApplyRemove("/a");
  EXPECT_EQ(state.chunks.Find(10)->refcount, 0u);
  EXPECT_EQ(state.chunks.Find(11)->refcount, 1u);
  state.RecomputeRefcounts();
  EXPECT_EQ(state.chunks.Find(11)->refcount, 1u);
  EXPECT_EQ(state.chunks.Find(20)->refcount, 1u);
  state.ApplyDropChunk(10);
  EXPECT_EQ(state.chunks.Find(10), nullptr);
}

}  // namespace gfs
