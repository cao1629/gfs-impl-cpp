#pragma once

#include <cstdint>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

namespace gfs {

struct BufferKey {
  std::string client_id;
  uint64_t sequence = 0;

  bool operator==(const BufferKey& other) const {
    return sequence == other.sequence && client_id == other.client_id;
  }
};

struct BufferKeyHash {
  size_t operator()(const BufferKey& key) const {
    return std::hash<std::string>()(key.client_id) ^
           (std::hash<uint64_t>()(key.sequence) * 1099511628211ull);
  }
};

class DataBuffer {
 public:
  explicit DataBuffer(uint64_t capacity);

  void Put(BufferKey key, std::string data);
  std::optional<std::string> Take(const BufferKey& key);
  std::optional<uint64_t> SizeOf(const BufferKey& key);
  uint64_t BytesInUse();
  size_t EntryCount();

 private:
  using Entry = std::pair<BufferKey, std::string>;

  std::mutex mutex_;
  uint64_t capacity_;
  uint64_t used_ = 0;
  std::list<Entry> lru_;
  std::unordered_map<BufferKey, std::list<Entry>::iterator, BufferKeyHash>
      index_;
};

}  // namespace gfs
