#include "chunkserver/data_buffer.h"

namespace gfs {

DataBuffer::DataBuffer(uint64_t capacity) : capacity_(capacity) {}

void DataBuffer::Put(BufferKey key, std::string data) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto existing = index_.find(key);
  if (existing != index_.end()) {
    used_ -= existing->second->second.size();
    lru_.erase(existing->second);
    index_.erase(existing);
  }
  while (!lru_.empty() && used_ + data.size() > capacity_) {
    auto& oldest = lru_.back();
    used_ -= oldest.second.size();
    index_.erase(oldest.first);
    lru_.pop_back();
  }
  used_ += data.size();
  lru_.emplace_front(std::move(key), std::move(data));
  index_[lru_.front().first] = lru_.begin();
}

std::optional<std::string> DataBuffer::Take(const BufferKey& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = index_.find(key);
  if (it == index_.end()) return std::nullopt;
  std::string data = std::move(it->second->second);
  used_ -= data.size();
  lru_.erase(it->second);
  index_.erase(it);
  return data;
}

std::optional<uint64_t> DataBuffer::SizeOf(const BufferKey& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = index_.find(key);
  if (it == index_.end()) return std::nullopt;
  return it->second->second.size();
}

uint64_t DataBuffer::BytesInUse() {
  std::lock_guard<std::mutex> lock(mutex_);
  return used_;
}

size_t DataBuffer::EntryCount() {
  std::lock_guard<std::mutex> lock(mutex_);
  return lru_.size();
}

}  // namespace gfs
