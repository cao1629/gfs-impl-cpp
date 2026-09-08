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

  void applyCreate(const std::string& path);
  void applyRename(const std::string& source, const std::string& target);
  void applyRemove(const std::string& path);
  void applyAllocHandle(uint64_t handle);
  void applyAddChunk(const std::string& path, uint64_t index, uint64_t handle);
  void applyReplaceChunk(const std::string& path, uint64_t index, uint64_t handle);
  void applyBumpVersion(uint64_t handle, uint64_t version);
  void applySnapshot(const std::string& source, const std::string& target);
  void applyDropChunk(uint64_t handle);
  void apply(const state::LogRecord& record);

  state::Checkpoint toCheckpoint() const;
  void load(const state::Checkpoint& checkpoint);
  void recomputeRefcounts();
};

}
