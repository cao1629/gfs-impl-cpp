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
  virtual bool openSegment(uint64_t number) = 0;
  virtual bool write(std::string_view bytes) = 0;
  virtual bool sync() = 0;
  virtual uint64_t size() const = 0;
};

class LocalFileSink : public LogSink {
 public:
  explicit LocalFileSink(std::string dir);
  ~LocalFileSink() override;

  bool openSegment(uint64_t number) override;
  bool write(std::string_view bytes) override;
  bool sync() override;
  uint64_t size() const override { return size_; }

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

  void addSink(std::unique_ptr<LogSink> sink);
  void enableCheckpoints(std::string dir, SnapshotFn snapshot);
  void open(uint64_t segment);
  void stop();

  uint64_t append(const state::LogRecord& record);
  void waitFlushed(uint64_t seq);
  uint64_t segment() const { return segment_; }

  static std::string segmentPath(const std::string& dir, uint64_t number);
  static std::vector<uint64_t> listSegments(const std::string& dir);
  static ReplayedSegment readSegment(const std::string& dir, uint64_t number);
  static void truncateSegment(const std::string& dir, uint64_t number, size_t bytes);

 private:
  void run();
  void writeBatch(std::vector<std::string>& batch, uint64_t last_seq);
  void rotate();

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

}
