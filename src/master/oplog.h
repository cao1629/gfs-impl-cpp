#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/config.h"
#include "master_state.pb.h"

namespace gfs {

class LogSink {
 public:
  virtual ~LogSink() = default;
  virtual bool OpenSegment(uint64_t number) = 0;
  virtual bool Write(std::string_view bytes) = 0;
  virtual bool Sync() = 0;
  virtual uint64_t Size() const = 0;
};

class LocalFileSink : public LogSink {
 public:
  explicit LocalFileSink(std::string dir);
  ~LocalFileSink() override;

  bool OpenSegment(uint64_t number) override;
  bool Write(std::string_view bytes) override;
  bool Sync() override;
  uint64_t Size() const override { return size_; }

 private:
  std::string dir_;
  int fd_ = -1;
  uint64_t size_ = 0;
};

struct ReplayedSegment {
  uint64_t number = 0;
  std::vector<state::LogRecord> records;
  bool torn_tail = false;
  size_t valid_bytes = 0;
};

class OpLog {
 public:
  using SnapshotFn = std::function<state::Checkpoint()>;

  OpLog(const Config& config, std::mutex& state_mutex);
  ~OpLog();

  void AddSink(std::unique_ptr<LogSink> sink);
  void EnableCheckpoints(std::string dir, SnapshotFn snapshot);
  void Open(uint64_t segment);
  void Stop();

  uint64_t Append(const state::LogRecord& record);
  void WaitFlushed(uint64_t seq);
  uint64_t Segment() const { return segment_; }

  static std::string SegmentPath(const std::string& dir, uint64_t number);
  static std::vector<uint64_t> ListSegments(const std::string& dir);
  static ReplayedSegment ReadSegment(const std::string& dir, uint64_t number);
  static void TruncateSegment(const std::string& dir, uint64_t number,
                              size_t bytes);

 private:
  void Run();
  void WriteBatch(std::vector<std::string>& batch, uint64_t last_seq);
  void Rotate();

  const Config& config_;
  std::mutex& state_mutex_;
  std::vector<std::unique_ptr<LogSink>> sinks_;
  std::string checkpoint_dir_;
  SnapshotFn snapshot_;

  std::mutex mutex_;
  std::condition_variable pending_cv_;
  std::condition_variable flushed_cv_;
  std::deque<std::pair<uint64_t, std::string>> pending_;
  uint64_t next_seq_ = 1;
  uint64_t flushed_seq_ = 0;
  uint64_t segment_ = 0;
  bool stopping_ = false;
  bool started_ = false;
  std::thread flusher_;
  std::thread checkpoint_writer_;
};

}  // namespace gfs
