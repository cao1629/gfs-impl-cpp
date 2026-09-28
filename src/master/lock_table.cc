#include "master/lock_table.h"

#include <algorithm>

#include "common/paths.h"

namespace gfs {

namespace {

size_t DepthOf(const std::string& path) {
  if (path == "/") return 0;
  return static_cast<size_t>(std::count(path.begin(), path.end(), '/'));
}

bool Before(const LockRequest& a, const LockRequest& b) {
  size_t da = DepthOf(a.path), db = DepthOf(b.path);
  if (da != db) return da < db;
  return a.path < b.path;
}

}  // namespace

LockSet::LockSet(LockTable* table, std::vector<LockRequest> held)
    : table_(table), held_(std::move(held)) {}

LockSet::~LockSet() { Release(); }

LockSet::LockSet(LockSet&& other) noexcept
    : table_(other.table_), held_(std::move(other.held_)) {
  other.table_ = nullptr;
  other.held_.clear();
}

LockSet& LockSet::operator=(LockSet&& other) noexcept {
  if (this != &other) {
    Release();
    table_ = other.table_;
    held_ = std::move(other.held_);
    other.table_ = nullptr;
    other.held_.clear();
  }
  return *this;
}

void LockSet::Release() {
  if (table_ != nullptr && !held_.empty()) table_->Release(held_);
  held_.clear();
  table_ = nullptr;
}

std::vector<LockRequest> LockTable::Normalize(
    std::vector<LockRequest> requests) {
  std::sort(requests.begin(), requests.end(), Before);
  std::vector<LockRequest> out;
  for (auto& r : requests) {
    if (!out.empty() && out.back().path == r.path) {
      if (r.mode == LockMode::kWrite) out.back().mode = LockMode::kWrite;
      continue;
    }
    out.push_back(std::move(r));
  }
  return out;
}

std::vector<LockRequest> LockTable::ForPath(std::string_view path,
                                            LockMode leaf) {
  std::vector<LockRequest> out;
  for (auto& ancestor : AncestorsOf(path))
    out.push_back({ancestor, LockMode::kRead});
  out.push_back({std::string(path), leaf});
  return out;
}

std::vector<LockRequest> LockTable::ForPaths(std::string_view first,
                                             LockMode first_mode,
                                             std::string_view second,
                                             LockMode second_mode) {
  auto out = ForPath(first, first_mode);
  auto more = ForPath(second, second_mode);
  out.insert(out.end(), more.begin(), more.end());
  return out;
}

LockSet LockTable::Acquire(std::vector<LockRequest> requests) {
  auto ordered = Normalize(std::move(requests));
  for (auto& r : ordered) LockOne(r);
  return LockSet(this, std::move(ordered));
}

void LockTable::Release(const std::vector<LockRequest>& held) {
  for (auto it = held.rbegin(); it != held.rend(); ++it) UnlockOne(*it);
}

size_t LockTable::ActiveEntries() {
  std::lock_guard<std::mutex> lock(mutex_);
  return entries_.size();
}

LockTable::Entry& LockTable::EntryLocked(const std::string& path) {
  auto it = entries_.find(path);
  if (it == entries_.end())
    it = entries_.emplace(path, std::make_unique<Entry>()).first;
  return *it->second;
}

void LockTable::LockOne(const LockRequest& request) {
  std::unique_lock<std::mutex> lock(mutex_);
  Entry& entry = EntryLocked(request.path);
  entry.refs += 1;
  if (request.mode == LockMode::kWrite) {
    while (entry.readers > 0 || entry.writer) {
      entry.waiting_writers += 1;
      entry.cv.wait(lock);
      entry.waiting_writers -= 1;
    }
    entry.writer = true;
  } else {
    while (entry.writer || entry.waiting_writers > 0) entry.cv.wait(lock);
    entry.readers += 1;
  }
}

void LockTable::UnlockOne(const LockRequest& request) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = entries_.find(request.path);
  if (it == entries_.end()) return;
  Entry& entry = *it->second;
  if (request.mode == LockMode::kWrite) {
    entry.writer = false;
  } else {
    entry.readers -= 1;
  }
  entry.refs -= 1;
  entry.cv.notify_all();
  if (entry.refs == 0) entries_.erase(it);
}

}  // namespace gfs
