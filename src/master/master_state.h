#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include "master/chunk_table.h"
#include "master/namespace.h"
#include "master_state.pb.h"

namespace gfs {

struct MasterState {
  std::mutex mutex;
  Namespace files;
  ChunkTable chunks;
  uint64_t next_handle = 1;

  void ApplyCreate(const std::string& path);
  void ApplyRename(const std::string& source, const std::string& target);
  void ApplyRemove(const std::string& path);
  void ApplyAllocHandle(uint64_t handle);
  void ApplyAddChunk(const std::string& path, uint64_t index, uint64_t handle);
  void ApplyReplaceChunk(const std::string& path, uint64_t index,
                         uint64_t handle);
  void ApplyBumpVersion(uint64_t handle, uint64_t version);
  void ApplySnapshot(const std::string& source, const std::string& target);
  void ApplyDropChunk(uint64_t handle);
  void Apply(const state::LogRecord& record);

  state::Checkpoint ToCheckpoint() const;
  void Load(const state::Checkpoint& checkpoint);
  void RecomputeRefcounts();
};

}  // namespace gfs
