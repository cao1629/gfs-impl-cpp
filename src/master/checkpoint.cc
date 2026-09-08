#include "master/checkpoint.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "common/framing.h"
#include "common/logging.h"
#include "master/oplog.h"

namespace gfs {

namespace fs = std::filesystem;

namespace {

std::string numbered(const std::string& prefix, uint64_t number) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%s%06llu", prefix.c_str(), static_cast<unsigned long long>(number));
  return buf;
}

bool parseNumbered(const std::string& name, const std::string& prefix, uint64_t* number) {
  if (name.size() != prefix.size() + 6 || name.compare(0, prefix.size(), prefix) != 0) return false;
  uint64_t value = 0;
  for (size_t i = prefix.size(); i < name.size(); ++i) {
    if (name[i] < '0' || name[i] > '9') return false;
    value = value * 10 + static_cast<uint64_t>(name[i] - '0');
  }
  *number = value;
  return true;
}

bool writeWholeFile(const std::string& path, const std::string& bytes) {
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return false;
  size_t done = 0;
  while (done < bytes.size()) {
    ssize_t n = ::write(fd, bytes.data() + done, bytes.size() - done);
    if (n <= 0) {
      ::close(fd);
      return false;
    }
    done += static_cast<size_t>(n);
  }
  bool ok = ::fsync(fd) == 0;
  ::close(fd);
  return ok;
}

}

std::string Checkpointer::path(const std::string& dir, uint64_t number) {
  return dir + "/" + numbered("checkpoint.", number);
}

bool Checkpointer::write(const std::string& dir, uint64_t number, const state::Checkpoint& checkpoint) {
  std::string payload;
  if (!checkpoint.SerializeToString(&payload)) return false;
  std::string final_path = path(dir, number);
  std::string tmp_path = final_path + ".tmp";
  if (!writeWholeFile(tmp_path, encodeRecord(payload))) {
    GFS_LOG_ERROR << "failed to write " << tmp_path;
    return false;
  }
  if (std::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
    GFS_LOG_ERROR << "failed to rename " << tmp_path;
    return false;
  }
  int dfd = ::open(dir.c_str(), O_RDONLY);
  if (dfd >= 0) {
    ::fsync(dfd);
    ::close(dfd);
  }
  GFS_LOG_INFO << "wrote " << final_path << " with " << checkpoint.files_size() << " files and " << checkpoint.chunks_size() << " chunks";
  return true;
}

std::vector<uint64_t> Checkpointer::list(const std::string& dir) {
  std::vector<uint64_t> numbers;
  std::error_code ec;
  for (const auto& entry : fs::directory_iterator(dir, ec)) {
    uint64_t number = 0;
    if (parseNumbered(entry.path().filename().string(), "checkpoint.", &number)) numbers.push_back(number);
  }
  std::sort(numbers.begin(), numbers.end());
  return numbers;
}

uint64_t Checkpointer::loadLatest(const std::string& dir, state::Checkpoint* checkpoint) {
  auto numbers = list(dir);
  for (auto it = numbers.rbegin(); it != numbers.rend(); ++it) {
    std::ifstream in(path(dir, *it), std::ios::binary);
    std::stringstream buffer;
    buffer << in.rdbuf();
    DecodedRecords decoded = decodeRecords(buffer.str());
    if (decoded.payloads.size() != 1 || decoded.torn_tail) {
      GFS_LOG_WARN << "ignoring damaged checkpoint " << *it;
      continue;
    }
    if (!checkpoint->ParseFromString(decoded.payloads[0])) {
      GFS_LOG_WARN << "ignoring unparsable checkpoint " << *it;
      continue;
    }
    return *it;
  }
  return 0;
}

void Checkpointer::prune(const std::string& dir, uint64_t newest, size_t keep) {
  std::error_code ec;
  for (uint64_t segment : OpLog::listSegments(dir)) {
    if (segment < newest) fs::remove(OpLog::segmentPath(dir, segment), ec);
  }
  auto numbers = list(dir);
  if (numbers.size() > keep) {
    for (size_t i = 0; i + keep < numbers.size(); ++i) fs::remove(path(dir, numbers[i]), ec);
  }
}

}
