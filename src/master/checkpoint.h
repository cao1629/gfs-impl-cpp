#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "master_state.pb.h"

namespace gfs {

class Checkpointer {
 public:
  static std::string Path(const std::string& dir, uint64_t number);
  static bool Write(const std::string& dir, uint64_t number,
                    const state::Checkpoint& checkpoint);
  static uint64_t LoadLatest(const std::string& dir,
                             state::Checkpoint* checkpoint);
  static std::vector<uint64_t> List(const std::string& dir);
  static void Prune(const std::string& dir, uint64_t newest, size_t keep);
};

}  // namespace gfs
