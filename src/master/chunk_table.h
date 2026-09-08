#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>

#include "common/clock.h"

namespace gfs {

struct Lease {
  std::string primary;
  TimePoint expiry;
  bool revoked = false;
};

struct ChunkMeta {
  uint64_t version = 1;
  uint32_t refcount = 0;
  bool pending = false;
  bool granting = false;
  std::map<std::string, TimePoint> locations;
  std::optional<Lease> lease;
};

class ChunkTable {
 public:
  ChunkMeta* find(uint64_t handle);
  const ChunkMeta* find(uint64_t handle) const;
  ChunkMeta& create(uint64_t handle, uint64_t version);
  void erase(uint64_t handle);

  bool addLocation(uint64_t handle, const std::string& chunkserver, TimePoint when);
  bool removeLocation(uint64_t handle, const std::string& chunkserver);
  std::set<uint64_t> heldBy(const std::string& chunkserver) const;
  size_t heldCount(const std::string& chunkserver) const;

  template <typename F>
  void forEach(F f) {
    for (auto& [handle, meta] : chunks_) f(handle, meta);
  }

  template <typename F>
  void forEach(F f) const {
    for (const auto& [handle, meta] : chunks_) f(handle, meta);
  }

  size_t size() const { return chunks_.size(); }
  void clear();

 private:
  std::unordered_map<uint64_t, ChunkMeta> chunks_;
  std::unordered_map<std::string, std::set<uint64_t>> held_by_;
};

}
