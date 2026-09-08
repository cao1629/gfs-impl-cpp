#include "master/master_state.h"

#include <algorithm>

#include "common/logging.h"
#include "common/paths.h"

namespace gfs {

void MasterState::applyCreate(const std::string& path) { files.insert(path, FileMeta{}); }

void MasterState::applyRename(const std::string& source, const std::string& target) {
  if (!files.exists(source) && !files.isDirectory(source)) return;
  files.renameSubtree(source, target);
}

void MasterState::applyRemove(const std::string& path) {
  FileMeta* meta = files.find(path);
  if (meta == nullptr) return;
  for (uint64_t handle : meta->chunks) {
    ChunkMeta* chunk = chunks.find(handle);
    if (chunk != nullptr && chunk->refcount > 0) chunk->refcount -= 1;
  }
  files.erase(path);
}

void MasterState::applyAllocHandle(uint64_t handle) { next_handle = std::max(next_handle, handle + 1); }

void MasterState::applyAddChunk(const std::string& path, uint64_t index, uint64_t handle) {
  FileMeta* meta = files.find(path);
  if (meta == nullptr) return;
  if (index < meta->chunks.size()) {
    if (meta->chunks[index] == handle) return;
    applyReplaceChunk(path, index, handle);
    return;
  }
  if (index != meta->chunks.size()) return;
  meta->chunks.push_back(handle);
  ChunkMeta& chunk = chunks.create(handle, 1);
  chunk.pending = false;
  chunk.refcount += 1;
  applyAllocHandle(handle);
}

void MasterState::applyReplaceChunk(const std::string& path, uint64_t index, uint64_t handle) {
  FileMeta* meta = files.find(path);
  if (meta == nullptr || index >= meta->chunks.size()) return;
  uint64_t old = meta->chunks[index];
  if (old == handle) return;
  ChunkMeta* previous = chunks.find(old);
  if (previous != nullptr && previous->refcount > 0) previous->refcount -= 1;
  meta->chunks[index] = handle;
  ChunkMeta& chunk = chunks.create(handle, 1);
  chunk.pending = false;
  chunk.refcount += 1;
  applyAllocHandle(handle);
}

void MasterState::applyBumpVersion(uint64_t handle, uint64_t version) {
  ChunkMeta* chunk = chunks.find(handle);
  if (chunk == nullptr) return;
  chunk->version = std::max(chunk->version, version);
}

void MasterState::applySnapshot(const std::string& source, const std::string& target) {
  for (auto& [path, meta] : files.subtree(source, true)) {
    std::string copied = path == source ? target : childPrefix(target) + path.substr(childPrefix(source).size());
    if (files.exists(copied)) continue;
    for (uint64_t handle : meta.chunks) {
      ChunkMeta* chunk = chunks.find(handle);
      if (chunk != nullptr) chunk->refcount += 1;
    }
    files.insert(copied, meta);
  }
}

void MasterState::applyDropChunk(uint64_t handle) { chunks.erase(handle); }

void MasterState::apply(const state::LogRecord& record) {
  switch (record.body_case()) {
    case state::LogRecord::kCreate: applyCreate(record.create().path()); break;
    case state::LogRecord::kRename: applyRename(record.rename().source(), record.rename().target()); break;
    case state::LogRecord::kRemove: applyRemove(record.remove().path()); break;
    case state::LogRecord::kAllocHandle: applyAllocHandle(record.alloc_handle().handle()); break;
    case state::LogRecord::kAddChunk: applyAddChunk(record.add_chunk().path(), record.add_chunk().index(), record.add_chunk().handle()); break;
    case state::LogRecord::kReplaceChunk: applyReplaceChunk(record.replace_chunk().path(), record.replace_chunk().index(), record.replace_chunk().handle()); break;
    case state::LogRecord::kBumpVersion: applyBumpVersion(record.bump_version().handle(), record.bump_version().version()); break;
    case state::LogRecord::kSnapshot: applySnapshot(record.snapshot().source(), record.snapshot().target()); break;
    case state::LogRecord::kDropChunk: applyDropChunk(record.drop_chunk().handle()); break;
    case state::LogRecord::BODY_NOT_SET: GFS_LOG_WARN << "log record without a body"; break;
  }
}

state::Checkpoint MasterState::toCheckpoint() const {
  state::Checkpoint checkpoint;
  checkpoint.set_next_chunk_handle(next_handle);
  files.forEach([&](const std::string& path, const FileMeta& meta) {
    state::FileEntry* entry = checkpoint.add_files();
    entry->set_path(path);
    for (uint64_t handle : meta.chunks) entry->add_chunk_handles(handle);
  });
  chunks.forEach([&](uint64_t handle, const ChunkMeta& meta) {
    if (meta.pending) return;
    state::ChunkEntry* entry = checkpoint.add_chunks();
    entry->set_handle(handle);
    entry->set_version(meta.version);
  });
  return checkpoint;
}

void MasterState::load(const state::Checkpoint& checkpoint) {
  files.clear();
  chunks.clear();
  next_handle = std::max<uint64_t>(1, checkpoint.next_chunk_handle());
  for (const auto& entry : checkpoint.files()) {
    FileMeta meta;
    for (uint64_t handle : entry.chunk_handles()) meta.chunks.push_back(handle);
    files.insert(entry.path(), std::move(meta));
  }
  for (const auto& entry : checkpoint.chunks()) {
    chunks.create(entry.handle(), entry.version());
    applyAllocHandle(entry.handle());
  }
}

void MasterState::recomputeRefcounts() {
  chunks.forEach([](uint64_t, ChunkMeta& meta) { meta.refcount = 0; });
  files.forEach([&](const std::string&, const FileMeta& meta) {
    for (uint64_t handle : meta.chunks) {
      ChunkMeta& chunk = chunks.create(handle, 1);
      chunk.refcount += 1;
      applyAllocHandle(handle);
    }
  });
}

}
