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
  ChunkStore(std::string data_dir, uint64_t chunk_size,
             uint64_t checksum_block_size);
  ~ChunkStore();

  ChunkStore(const ChunkStore&) = delete;
  ChunkStore& operator=(const ChunkStore&) = delete;

  void Scan();

  rpc::ResultCode Create(uint64_t handle, uint64_t version);
  rpc::ResultCode CreateCopy(uint64_t handle, uint64_t version,
                             uint64_t copy_from);
  rpc::ResultCode Read(uint64_t handle, uint64_t offset, uint64_t length,
                       std::string* out);
  rpc::ResultCode Write(uint64_t handle, uint64_t offset,
                        std::string_view data);
  rpc::ResultCode Pad(uint64_t handle, uint64_t from_offset);
  rpc::ResultCode Length(uint64_t handle, uint64_t* out);
  std::optional<uint64_t> Version(uint64_t handle);
  rpc::ResultCode SetVersion(uint64_t handle, uint64_t version);
  bool Contains(uint64_t handle);
  bool Remove(uint64_t handle);
  std::vector<ChunkListing> List();
  MutationLock LockForMutation(uint64_t handle);

  std::vector<uint64_t> CorruptHandles();
  void ClearCorrupt(const std::vector<uint64_t>& handles);

  uint64_t ChunkSize() const { return chunk_size_; }
  uint64_t ChecksumBlockSize() const { return block_size_; }
  std::string ChunkPath(uint64_t handle) const;
  std::string MetaPath(uint64_t handle) const;

 private:
  std::shared_ptr<ChunkEntry> Find(uint64_t handle);
  std::shared_ptr<ChunkEntry> OpenEntry(uint64_t handle, bool create_new,
                                        uint64_t version);
  bool RecomputeBlocks(ChunkEntry& entry, uint64_t first_block,
                       uint64_t last_block);
  void MarkCorrupt(uint64_t handle);

  std::string data_dir_;
  uint64_t chunk_size_;
  uint64_t block_size_;
  std::mutex table_mutex_;
  std::unordered_map<uint64_t, std::shared_ptr<ChunkEntry>> entries_;
  std::mutex corrupt_mutex_;
  std::unordered_set<uint64_t> corrupt_;
};

std::string LoadOrCreateChunkserverId(const std::string& data_dir);

}  // namespace gfs
