#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <thread>

#include "master/checkpoint.h"
#include "master/master_state.h"
#include "master/oplog.h"

namespace gfs {

namespace fs = std::filesystem;

namespace {

std::string freshDir(const std::string& name) {
  std::string dir = (fs::temp_directory_path() / ("gfs-oplog-" + name + "-" + std::to_string(::getpid()))).string();
  fs::remove_all(dir);
  fs::create_directories(dir);
  return dir;
}

state::LogRecord createRecord(const std::string& path) {
  state::LogRecord record;
  record.mutable_create()->set_path(path);
  return record;
}

}

TEST(OpLog, AppendsFlushesAndReplays) {
  std::string dir = freshDir("replay");
  Config config;
  config.log_flush_max_delay = Millis(2);
  std::mutex state_mutex;
  {
    OpLog log(config, state_mutex);
    log.addSink(std::make_unique<LocalFileSink>(dir));
    log.open(1);
    uint64_t seq = 0;
    for (int i = 0; i < 5; ++i) seq = log.append(createRecord("/f" + std::to_string(i)));
    log.waitFlushed(seq);
  }
  ReplayedSegment segment = OpLog::readSegment(dir, 1);
  ASSERT_EQ(segment.records.size(), 5u);
  EXPECT_EQ(segment.records[4].create().path(), "/f4");
  EXPECT_FALSE(segment.torn_tail);

  std::ofstream(OpLog::segmentPath(dir, 1), std::ios::binary | std::ios::app) << "\x09\x00\x00\x00junk";
  ReplayedSegment torn = OpLog::readSegment(dir, 1);
  EXPECT_EQ(torn.records.size(), 5u);
  EXPECT_TRUE(torn.torn_tail);
  OpLog::truncateSegment(dir, 1, torn.valid_bytes);
  EXPECT_FALSE(OpLog::readSegment(dir, 1).torn_tail);
  EXPECT_EQ(OpLog::listSegments(dir), std::vector<uint64_t>{1});
}

TEST(OpLog, RotatesIntoCheckpointsThatRecoveryLoads) {
  std::string dir = freshDir("rotate");
  Config config;
  config.log_flush_max_delay = Millis(2);
  config.checkpoint_log_threshold = 200;
  MasterState state;
  {
    OpLog log(config, state.mutex);
    log.addSink(std::make_unique<LocalFileSink>(dir));
    log.enableCheckpoints(dir, [&] { return state.toCheckpoint(); });
    log.open(1);
    for (int i = 0; i < 40; ++i) {
      uint64_t seq = 0;
      {
        std::lock_guard<std::mutex> lock(state.mutex);
        std::string path = "/file" + std::to_string(i);
        state.applyCreate(path);
        seq = log.append(createRecord(path));
      }
      log.waitFlushed(seq);
    }
    EXPECT_GT(log.segment(), 1u);
  }
  auto checkpoints = Checkpointer::list(dir);
  ASSERT_FALSE(checkpoints.empty());
  EXPECT_LE(checkpoints.size(), 2u);
  std::ofstream(Checkpointer::path(dir, 999) + ".tmp") << "half written";
  state::Checkpoint loaded;
  uint64_t number = Checkpointer::loadLatest(dir, &loaded);
  EXPECT_EQ(number, checkpoints.back());
  MasterState recovered;
  recovered.load(loaded);
  for (uint64_t segment : OpLog::listSegments(dir)) {
    EXPECT_GE(segment, number);
    for (const auto& record : OpLog::readSegment(dir, segment).records) recovered.apply(record);
  }
  EXPECT_EQ(recovered.files.size(), 40u);
  EXPECT_TRUE(recovered.files.exists("/file39"));
}

TEST(Checkpointer, IgnoresDamagedFiles) {
  std::string dir = freshDir("damaged");
  state::Checkpoint good;
  good.set_next_chunk_handle(77);
  good.add_files()->set_path("/keep");
  ASSERT_TRUE(Checkpointer::write(dir, 3, good));
  std::ofstream(Checkpointer::path(dir, 4), std::ios::binary) << "not a checkpoint";
  state::Checkpoint loaded;
  EXPECT_EQ(Checkpointer::loadLatest(dir, &loaded), 3u);
  EXPECT_EQ(loaded.next_chunk_handle(), 77u);
  EXPECT_EQ(loaded.files(0).path(), "/keep");
}

TEST(MasterState, ApplyIsIdempotentAndRefcountsFollowSnapshots) {
  MasterState state;
  state.applyCreate("/a");
  state.applyCreate("/a");
  state.applyAddChunk("/a", 0, 10);
  state.applyAddChunk("/a", 0, 10);
  state.applyAddChunk("/a", 1, 11);
  EXPECT_EQ(state.files.find("/a")->chunks, (std::vector<uint64_t>{10, 11}));
  EXPECT_EQ(state.next_handle, 12u);
  state.applySnapshot("/a", "/b");
  state.applySnapshot("/a", "/b");
  EXPECT_EQ(state.chunks.find(10)->refcount, 2u);
  state.applyReplaceChunk("/b", 0, 20);
  EXPECT_EQ(state.chunks.find(10)->refcount, 1u);
  EXPECT_EQ(state.chunks.find(20)->refcount, 1u);
  state.applyBumpVersion(10, 5);
  state.applyBumpVersion(10, 3);
  EXPECT_EQ(state.chunks.find(10)->version, 5u);
  state.applyRemove("/a");
  state.applyRemove("/a");
  EXPECT_EQ(state.chunks.find(10)->refcount, 0u);
  EXPECT_EQ(state.chunks.find(11)->refcount, 1u);
  state.recomputeRefcounts();
  EXPECT_EQ(state.chunks.find(11)->refcount, 1u);
  EXPECT_EQ(state.chunks.find(20)->refcount, 1u);
  state.applyDropChunk(10);
  EXPECT_EQ(state.chunks.find(10), nullptr);
}

}
