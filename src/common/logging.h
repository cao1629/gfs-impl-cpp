#pragma once

#include <sstream>
#include <string>

namespace gfs {

enum class LogLevel { kInfo, kWarn, kError };

void setLogTag(const std::string& tag);

class LogLine {
 public:
  explicit LogLine(LogLevel level);
  ~LogLine();
  template <typename T>
  LogLine& operator<<(const T& value) {
    stream_ << value;
    return *this;
  }

 private:
  LogLevel level_;
  std::ostringstream stream_;
};

}

#define GFS_LOG_INFO ::gfs::LogLine(::gfs::LogLevel::kInfo)
#define GFS_LOG_WARN ::gfs::LogLine(::gfs::LogLevel::kWarn)
#define GFS_LOG_ERROR ::gfs::LogLine(::gfs::LogLevel::kError)
