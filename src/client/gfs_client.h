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

  bool ok() const { return code == ErrorCode::kOk; }
  static Status Ok() { return {}; }
  static Status Error(ErrorCode code, std::string message) { return {code, std::move(message)}; }
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

  Status create(const std::string& path);
  Status remove(const std::string& path);
  Status rename(const std::string& source, const std::string& target);
  Status snapshot(const std::string& source, const std::string& target);
  Status list(const std::string& directory, std::vector<DirEntry>* entries, bool include_hidden = false);
  Status open(const std::string& path, FileInfo* info);
  Status length(const std::string& path, uint64_t* length);
  Status read(const std::string& path, uint64_t offset, uint64_t length, std::string* data);
  Status write(const std::string& path, uint64_t offset, std::string_view data);
  Status recordAppend(const std::string& path, std::string_view data, uint64_t* offset);

  uint64_t chunkSize();
  const std::string& clientId() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}
