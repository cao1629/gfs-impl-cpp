#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace gfs {

enum class LockMode { kRead, kWrite };

struct LockRequest {
  std::string path;
  LockMode mode;
};

class LockTable;

class LockSet {
 public:
  LockSet() = default;
  LockSet(LockTable* table, std::vector<LockRequest> held);
  ~LockSet();

  LockSet(LockSet&& other) noexcept;
  LockSet& operator=(LockSet&& other) noexcept;
  LockSet(const LockSet&) = delete;
  LockSet& operator=(const LockSet&) = delete;

  void release();
  bool empty() const { return held_.empty(); }
  const std::vector<LockRequest>& held() const { return held_; }

 private:
  LockTable* table_ = nullptr;
  std::vector<LockRequest> held_;
};

class LockTable {
 public:
  LockSet acquire(std::vector<LockRequest> requests);
  void release(const std::vector<LockRequest>& held);
  size_t activeEntries();

  static std::vector<LockRequest> normalize(std::vector<LockRequest> requests);
  static std::vector<LockRequest> forPath(std::string_view path, LockMode leaf);
  static std::vector<LockRequest> forPaths(std::string_view first, LockMode first_mode, std::string_view second, LockMode second_mode);

 private:
  struct Entry {
    int readers = 0;
    bool writer = false;
    int waiting_writers = 0;
    int refs = 0;
    std::condition_variable cv;
  };

  Entry& entryLocked(const std::string& path);
  void lockOne(const LockRequest& request);
  void unlockOne(const LockRequest& request);

  std::mutex mutex_;
  std::unordered_map<std::string, std::unique_ptr<Entry>> entries_;
};

}
