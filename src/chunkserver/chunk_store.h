#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gfs.pb.h"

namespace gfs {

struct ChunkEntry;

struct ChunkListing {
  uint64_t handle = 0;
  uint64_t version = 0;
  uint64_t length = 0;
};

class MutationLock {
 public:
  MutationLock() = default;
  explicit MutationLock(std::shared_ptr<ChunkEntry> entry);
  MutationLock(MutationLock&&) = default;
  MutationLock& operator=(MutationLock&&) = default;
  explicit operator bool() const { return entry_ != nullptr; }

 private:
  std::shared_ptr<ChunkEntry> entry_;
  std::unique_lock<std::mutex> lock_;
};

class ChunkStore {
 public:
  ChunkStore(std::string data_dir, uint64_t chunk_size, uint64_t checksum_block_size);
  ~ChunkStore();

  ChunkStore(const ChunkStore&) = delete;
  ChunkStore& operator=(const ChunkStore&) = delete;

  void scan();

  rpc::ResultCode create(uint64_t handle, uint64_t version);
  rpc::ResultCode createCopy(uint64_t handle, uint64_t version, uint64_t copy_from);
  rpc::ResultCode read(uint64_t handle, uint64_t offset, uint64_t length, std::string* out);
  rpc::ResultCode write(uint64_t handle, uint64_t offset, std::string_view data);
  rpc::ResultCode pad(uint64_t handle, uint64_t from_offset);
  rpc::ResultCode length(uint64_t handle, uint64_t* out);
  std::optional<uint64_t> version(uint64_t handle);
  rpc::ResultCode setVersion(uint64_t handle, uint64_t version);
  bool contains(uint64_t handle);
  bool remove(uint64_t handle);
  std::vector<ChunkListing> list();
  MutationLock lockForMutation(uint64_t handle);

  std::vector<uint64_t> corruptHandles();
  void clearCorrupt(const std::vector<uint64_t>& handles);

  uint64_t chunkSize() const { return chunk_size_; }
  uint64_t checksumBlockSize() const { return block_size_; }
  std::string chunkPath(uint64_t handle) const;
  std::string metaPath(uint64_t handle) const;

 private:
  std::shared_ptr<ChunkEntry> find(uint64_t handle);
  std::shared_ptr<ChunkEntry> openEntry(uint64_t handle, bool create_new, uint64_t version);
  bool recomputeBlocks(ChunkEntry& entry, uint64_t first_block, uint64_t last_block);
  void markCorrupt(uint64_t handle);

  std::string data_dir_;
  uint64_t chunk_size_;
  uint64_t block_size_;
  std::mutex table_mutex_;
  std::unordered_map<uint64_t, std::shared_ptr<ChunkEntry>> entries_;
  std::mutex corrupt_mutex_;
  std::unordered_set<uint64_t> corrupt_;
};

std::string loadOrCreateChunkserverId(const std::string& data_dir);

}
