#include "master/oplog.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "common/framing.h"
#include "common/logging.h"
#include "master/checkpoint.h"

namespace gfs {

namespace fs = std::filesystem;

LocalFileSink::LocalFileSink(std::string dir) : dir_(std::move(dir)) {}

LocalFileSink::~LocalFileSink() {
  if (fd_ >= 0) ::close(fd_);
}

bool LocalFileSink::openSegment(uint64_t number) {
  if (fd_ >= 0) {
    ::fsync(fd_);
    ::close(fd_);
  }
  std::string path = OpLog::segmentPath(dir_, number);
  fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd_ < 0) {
    GFS_LOG_ERROR << "cannot open log segment " << path;
    return false;
  }
  struct stat st{};
  size_ = ::fstat(fd_, &st) == 0 ? static_cast<uint64_t>(st.st_size) : 0;
  return true;
}

bool LocalFileSink::write(std::string_view bytes) {
  size_t done = 0;
  while (done < bytes.size()) {
    ssize_t n = ::write(fd_, bytes.data() + done, bytes.size() - done);
    if (n <= 0) return false;
    done += static_cast<size_t>(n);
  }
  size_ += bytes.size();
  return true;
}

bool LocalFileSink::sync() { return ::fsync(fd_) == 0; }

OpLog::OpLog(const Config& config, std::mutex& state_mutex) : config_(config), state_mutex_(state_mutex) {}

OpLog::~OpLog() { stop(); }

void OpLog::addSink(std::unique_ptr<LogSink> sink) { sinks_.push_back(std::move(sink)); }

void OpLog::enableCheckpoints(std::string dir, SnapshotFn snapshot) {
  checkpoint_dir_ = std::move(dir);
  snapshot_ = std::move(snapshot);
}

void OpLog::open(uint64_t segment) {
  segment_ = segment;
  for (auto& sink : sinks_) sink->openSegment(segment);
  started_ = true;
  flusher_ = std::thread([this] { run(); });
}

void OpLog::stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return;
    stopping_ = true;
  }
  pending_cv_.notify_all();
  if (flusher_.joinable()) flusher_.join();
  if (checkpoint_writer_.joinable()) checkpoint_writer_.join();
  for (auto& sink : sinks_) sink->sync();
}

uint64_t OpLog::append(const state::LogRecord& record) {
  std::string payload;
  if (!record.SerializeToString(&payload)) GFS_LOG_ERROR << "could not serialize a log record";
  uint64_t seq = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    seq = next_seq_++;
    pending_.emplace_back(seq, encodeRecord(payload));
  }
  pending_cv_.notify_one();
  return seq;
}

void OpLog::waitFlushed(uint64_t seq) {
  std::unique_lock<std::mutex> lock(mutex_);
  flushed_cv_.wait(lock, [&] { return flushed_seq_ >= seq || stopping_; });
}

void OpLog::run() {
  while (true) {
    std::vector<std::string> batch;
    uint64_t last_seq = 0;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      pending_cv_.wait(lock, [&] { return stopping_ || !pending_.empty(); });
      if (pending_.empty() && stopping_) return;
      if (pending_.size() < config_.log_flush_batch_size) {
        pending_cv_.wait_for(lock, config_.log_flush_max_delay, [&] { return stopping_ || pending_.size() >= config_.log_flush_batch_size; });
      }
      for (auto& [seq, bytes] : pending_) {
        batch.push_back(std::move(bytes));
        last_seq = seq;
      }
      pending_.clear();
    }
    writeBatch(batch, last_seq);
    if (!checkpoint_dir_.empty() && !sinks_.empty() && sinks_.front()->size() >= config_.checkpoint_log_threshold) rotate();
  }
}

void OpLog::writeBatch(std::vector<std::string>& batch, uint64_t last_seq) {
  if (batch.empty()) return;
  std::string joined;
  for (auto& bytes : batch) joined += bytes;
  for (auto& sink : sinks_) {
    if (!sink->write(joined) || !sink->sync()) GFS_LOG_ERROR << "log sink write failed";
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    flushed_seq_ = std::max(flushed_seq_, last_seq);
  }
  flushed_cv_.notify_all();
}

void OpLog::rotate() {
  state::Checkpoint checkpoint;
  uint64_t new_segment = 0;
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    std::vector<std::string> batch;
    uint64_t last_seq = 0;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (auto& [seq, bytes] : pending_) {
        batch.push_back(std::move(bytes));
        last_seq = seq;
      }
      pending_.clear();
    }
    writeBatch(batch, last_seq);
    checkpoint = snapshot_();
    new_segment = segment_ + 1;
    for (auto& sink : sinks_) sink->openSegment(new_segment);
    segment_ = new_segment;
  }
  if (checkpoint_writer_.joinable()) checkpoint_writer_.join();
  std::string dir = checkpoint_dir_;
  checkpoint_writer_ = std::thread([dir, new_segment, checkpoint = std::move(checkpoint)] {
    if (Checkpointer::write(dir, new_segment, checkpoint)) Checkpointer::prune(dir, new_segment, 2);
  });
}

std::string OpLog::segmentPath(const std::string& dir, uint64_t number) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "oplog.%06llu", static_cast<unsigned long long>(number));
  return dir + "/" + buf;
}

std::vector<uint64_t> OpLog::listSegments(const std::string& dir) {
  std::vector<uint64_t> numbers;
  std::error_code ec;
  for (const auto& entry : fs::directory_iterator(dir, ec)) {
    std::string name = entry.path().filename().string();
    if (name.size() != 12 || name.compare(0, 6, "oplog.") != 0) continue;
    uint64_t value = 0;
    bool digits = true;
    for (size_t i = 6; i < name.size(); ++i) {
      if (name[i] < '0' || name[i] > '9') digits = false;
      value = value * 10 + static_cast<uint64_t>(name[i] - '0');
    }
    if (digits) numbers.push_back(value);
  }
  std::sort(numbers.begin(), numbers.end());
  return numbers;
}

ReplayedSegment OpLog::readSegment(const std::string& dir, uint64_t number) {
  ReplayedSegment out;
  out.number = number;
  std::ifstream in(segmentPath(dir, number), std::ios::binary);
  std::stringstream buffer;
  buffer << in.rdbuf();
  std::string bytes = buffer.str();
  DecodedRecords decoded = decodeRecords(bytes);
  out.torn_tail = decoded.torn_tail;
  out.valid_bytes = decoded.consumed;
  for (const auto& payload : decoded.payloads) {
    state::LogRecord record;
    if (record.ParseFromString(payload)) out.records.push_back(std::move(record));
  }
  return out;
}

void OpLog::truncateSegment(const std::string& dir, uint64_t number, size_t bytes) {
  std::string path = segmentPath(dir, number);
  if (::truncate(path.c_str(), static_cast<off_t>(bytes)) != 0) GFS_LOG_WARN << "could not truncate " << path;
}

}
