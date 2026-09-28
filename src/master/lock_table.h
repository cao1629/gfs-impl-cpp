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

  void Release();
  bool Empty() const { return held_.empty(); }
  const std::vector<LockRequest>& Held() const { return held_; }

 private:
  LockTable* table_ = nullptr;
  std::vector<LockRequest> held_;
};

class LockTable {
 public:
  LockSet Acquire(std::vector<LockRequest> requests);
  void Release(const std::vector<LockRequest>& held);
  size_t ActiveEntries();

  static std::vector<LockRequest> Normalize(std::vector<LockRequest> requests);
  static std::vector<LockRequest> ForPath(std::string_view path, LockMode leaf);
  static std::vector<LockRequest> ForPaths(std::string_view first,
                                           LockMode first_mode,
                                           std::string_view second,
                                           LockMode second_mode);

 private:
  struct Entry {
    int readers = 0;
    bool writer = false;
    int waiting_writers = 0;
    int refs = 0;
    std::condition_variable cv;
  };

  Entry& EntryLocked(const std::string& path);
  void LockOne(const LockRequest& request);
  void UnlockOne(const LockRequest& request);

  std::mutex mutex_;
  std::unordered_map<std::string, std::unique_ptr<Entry>> entries_;
};

}  // namespace gfs
