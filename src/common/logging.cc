#include "common/logging.h"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <mutex>

namespace gfs {

namespace {

std::mutex& LogMutex() {
  static std::mutex m;
  return m;
}

std::string& LogTag() {
  static std::string tag;
  return tag;
}

const char* LevelName(LogLevel level) {
  switch (level) {
    case LogLevel::kInfo:
      return "I";
    case LogLevel::kWarn:
      return "W";
    case LogLevel::kError:
      return "E";
  }
  return "?";
}

}  // namespace

void SetLogTag(const std::string& tag) {
  std::lock_guard<std::mutex> lock(LogMutex());
  LogTag() = tag;
}

LogLine::LogLine(LogLevel level) : level_(level) {}

LogLine::~LogLine() {
  auto now = std::chrono::system_clock::now();
  auto secs = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch())
                .count() %
            1000;
  std::tm tm{};
  localtime_r(&secs, &tm);
  char stamp[32];
  std::strftime(stamp, sizeof(stamp), "%H:%M:%S", &tm);
  std::lock_guard<std::mutex> lock(LogMutex());
  std::fprintf(stderr, "%s %s.%03lld [%s] %s\n", LevelName(level_), stamp,
               static_cast<long long>(ms), LogTag().c_str(),
               stream_.str().c_str());
  std::fflush(stderr);
}

}  // namespace gfs
