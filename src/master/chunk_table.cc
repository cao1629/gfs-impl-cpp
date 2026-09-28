#include "master/chunk_table.h"

namespace gfs {

ChunkMeta* ChunkTable::Find(uint64_t handle) {
  auto it = chunks_.find(handle);
  return it == chunks_.end() ? nullptr : &it->second;
}

const ChunkMeta* ChunkTable::Find(uint64_t handle) const {
  auto it = chunks_.find(handle);
  return it == chunks_.end() ? nullptr : &it->second;
}

ChunkMeta& ChunkTable::Create(uint64_t handle, uint64_t version) {
  auto [it, inserted] = chunks_.try_emplace(handle);
  if (inserted) it->second.version = version;
  return it->second;
}

void ChunkTable::Erase(uint64_t handle) {
  auto it = chunks_.find(handle);
  if (it == chunks_.end()) return;
  for (const auto& [server, _] : it->second.locations) {
    auto held = held_by_.find(server);
    if (held != held_by_.end()) {
      held->second.erase(handle);
      if (held->second.empty()) held_by_.erase(held);
    }
  }
  chunks_.erase(it);
}

bool ChunkTable::AddLocation(uint64_t handle, const std::string& chunkserver,
                             TimePoint when) {
  ChunkMeta* meta = Find(handle);
  if (meta == nullptr) return false;
  auto [it, inserted] = meta->locations.try_emplace(chunkserver, when);
  if (inserted) held_by_[chunkserver].insert(handle);
  return inserted;
}

bool ChunkTable::RemoveLocation(uint64_t handle,
                                const std::string& chunkserver) {
  ChunkMeta* meta = Find(handle);
  if (meta == nullptr) return false;
  if (meta->locations.erase(chunkserver) == 0) return false;
  auto held = held_by_.find(chunkserver);
  if (held != held_by_.end()) {
    held->second.erase(handle);
    if (held->second.empty()) held_by_.erase(held);
  }
  return true;
}

std::set<uint64_t> ChunkTable::HeldBy(const std::string& chunkserver) const {
  auto it = held_by_.find(chunkserver);
  return it == held_by_.end() ? std::set<uint64_t>{} : it->second;
}

size_t ChunkTable::HeldCount(const std::string& chunkserver) const {
  auto it = held_by_.find(chunkserver);
  return it == held_by_.end() ? 0 : it->second.size();
}

void ChunkTable::Clear() {
  chunks_.clear();
  held_by_.clear();
}

}  // namespace gfs
