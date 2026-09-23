#include "master/master_state.h"

#include <algorithm>

#include "common/logging.h"
#include "common/paths.h"

namespace gfs {

void MasterState::ApplyCreate(const std::string& path) {
  files.Insert(path, FileMeta{});
}

void MasterState::ApplyRename(const std::string& source,
                              const std::string& target) {
  if (!files.Exists(source) && !files.IsDirectory(source)) return;
  files.RenameSubtree(source, target);
}

void MasterState::ApplyRemove(const std::string& path) {
  FileMeta* meta = files.Find(path);
  if (meta == nullptr) return;
  for (uint64_t handle : meta->chunks) {
    ChunkMeta* chunk = chunks.Find(handle);
    if (chunk != nullptr && chunk->refcount > 0) chunk->refcount -= 1;
  }
  files.Erase(path);
}

void MasterState::ApplyAllocHandle(uint64_t handle) {
  next_handle = std::max(next_handle, handle + 1);
}

void MasterState::ApplyAddChunk(const std::string& path, uint64_t index,
                                uint64_t handle) {
  FileMeta* meta = files.Find(path);
  if (meta == nullptr) return;
  if (index < meta->chunks.size()) {
    if (meta->chunks[index] == handle) return;
    ApplyReplaceChunk(path, index, handle);
    return;
  }
  if (index != meta->chunks.size()) return;
  meta->chunks.push_back(handle);
  ChunkMeta& chunk = chunks.Create(handle, 1);
  chunk.pending = false;
  chunk.refcount += 1;
  ApplyAllocHandle(handle);
}

void MasterState::ApplyReplaceChunk(const std::string& path, uint64_t index,
                                    uint64_t handle) {
  FileMeta* meta = files.Find(path);
  if (meta == nullptr || index >= meta->chunks.size()) return;
  uint64_t old = meta->chunks[index];
  if (old == handle) return;
  ChunkMeta* previous = chunks.Find(old);
  if (previous != nullptr && previous->refcount > 0) previous->refcount -= 1;
  meta->chunks[index] = handle;
  ChunkMeta& chunk = chunks.Create(handle, 1);
  chunk.pending = false;
  chunk.refcount += 1;
  ApplyAllocHandle(handle);
}

void MasterState::ApplyBumpVersion(uint64_t handle, uint64_t version) {
  ChunkMeta* chunk = chunks.Find(handle);
  if (chunk == nullptr) return;
  chunk->version = std::max(chunk->version, version);
}

void MasterState::ApplySnapshot(const std::string& source,
                                const std::string& target) {
  for (auto& [path, meta] : files.Subtree(source, true)) {
    std::string copied =
        path == source
            ? target
            : ChildPrefix(target) + path.substr(ChildPrefix(source).size());
    if (files.Exists(copied)) continue;
    for (uint64_t handle : meta.chunks) {
      ChunkMeta* chunk = chunks.Find(handle);
      if (chunk != nullptr) chunk->refcount += 1;
    }
    files.Insert(copied, meta);
  }
}

void MasterState::ApplyDropChunk(uint64_t handle) { chunks.Erase(handle); }

void MasterState::Apply(const state::LogRecord& record) {
  switch (record.body_case()) {
    case state::LogRecord::kCreate:
      ApplyCreate(record.create().path());
      break;
    case state::LogRecord::kRename:
      ApplyRename(record.rename().source(), record.rename().target());
      break;
    case state::LogRecord::kRemove:
      ApplyRemove(record.remove().path());
      break;
    case state::LogRecord::kAllocHandle:
      ApplyAllocHandle(record.alloc_handle().handle());
      break;
    case state::LogRecord::kAddChunk:
      ApplyAddChunk(record.add_chunk().path(), record.add_chunk().index(),
                    record.add_chunk().handle());
      break;
    case state::LogRecord::kReplaceChunk:
      ApplyReplaceChunk(record.replace_chunk().path(),
                        record.replace_chunk().index(),
                        record.replace_chunk().handle());
      break;
    case state::LogRecord::kBumpVersion:
      ApplyBumpVersion(record.bump_version().handle(),
                       record.bump_version().version());
      break;
    case state::LogRecord::kSnapshot:
      ApplySnapshot(record.snapshot().source(), record.snapshot().target());
      break;
    case state::LogRecord::kDropChunk:
      ApplyDropChunk(record.drop_chunk().handle());
      break;
    case state::LogRecord::BODY_NOT_SET:
      GFS_LOG_WARN << "log record without a body";
      break;
  }
}

state::Checkpoint MasterState::ToCheckpoint() const {
  state::Checkpoint checkpoint;
  checkpoint.set_next_chunk_handle(next_handle);
  files.ForEach([&](const std::string& path, const FileMeta& meta) {
    state::FileEntry* entry = checkpoint.add_files();
    entry->set_path(path);
    for (uint64_t handle : meta.chunks) entry->add_chunk_handles(handle);
  });
  chunks.ForEach([&](uint64_t handle, const ChunkMeta& meta) {
    if (meta.pending) return;
    state::ChunkEntry* entry = checkpoint.add_chunks();
    entry->set_handle(handle);
    entry->set_version(meta.version);
  });
  return checkpoint;
}

void MasterState::Load(const state::Checkpoint& checkpoint) {
  files.Clear();
  chunks.Clear();
  next_handle = std::max<uint64_t>(1, checkpoint.next_chunk_handle());
  for (const auto& entry : checkpoint.files()) {
    FileMeta meta;
    for (uint64_t handle : entry.chunk_handles()) meta.chunks.push_back(handle);
    files.Insert(entry.path(), std::move(meta));
  }
  for (const auto& entry : checkpoint.chunks()) {
    chunks.Create(entry.handle(), entry.version());
    ApplyAllocHandle(entry.handle());
  }
}

void MasterState::RecomputeRefcounts() {
  chunks.ForEach([](uint64_t, ChunkMeta& meta) { meta.refcount = 0; });
  files.ForEach([&](const std::string&, const FileMeta& meta) {
    for (uint64_t handle : meta.chunks) {
      ChunkMeta& chunk = chunks.Create(handle, 1);
      chunk.refcount += 1;
      ApplyAllocHandle(handle);
    }
  });
}

}  // namespace gfs
