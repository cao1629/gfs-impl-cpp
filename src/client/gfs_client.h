#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/config.h"

namespace gfs {

enum class ErrorCode {
  kOk,
  kNotFound,
  kAlreadyExists,
  kInvalidArgument,
  kUnavailable,
  kStale,
  kFailed,
};

struct Status {
  ErrorCode code = ErrorCode::kOk;
  std::string message;

  // NOLINTNEXTLINE(readability-identifier-naming)
  bool ok() const { return code == ErrorCode::kOk; }
  static Status Ok() { return {}; }
  static Status Error(ErrorCode code, std::string message) {
    return {code, std::move(message)};
  }
};

struct FileInfo {
  uint64_t chunk_count = 0;
};

struct DirEntry {
  std::string name;
  bool is_directory = false;
};

class Client {
 public:
  explicit Client(Config config);
  ~Client();

  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  Status Create(const std::string& path);
  Status Remove(const std::string& path);
  Status Rename(const std::string& source, const std::string& target);
  Status Snapshot(const std::string& source, const std::string& target);
  Status List(const std::string& directory, std::vector<DirEntry>* entries,
              bool include_hidden = false);
  Status Open(const std::string& path, FileInfo* info);
  Status Length(const std::string& path, uint64_t* length);
  Status Read(const std::string& path, uint64_t offset, uint64_t length,
              std::string* data);
  Status Write(const std::string& path, uint64_t offset, std::string_view data);
  Status RecordAppend(const std::string& path, std::string_view data,
                      uint64_t* offset);

  uint64_t ChunkSize();
  const std::string& ClientId() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace gfs
