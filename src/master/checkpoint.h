#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "master_state.pb.h"

namespace gfs {

class Checkpointer {
 public:
  static std::string path(const std::string& dir, uint64_t number);
  static bool write(const std::string& dir, uint64_t number, const state::Checkpoint& checkpoint);
  static uint64_t loadLatest(const std::string& dir, state::Checkpoint* checkpoint);
  static std::vector<uint64_t> list(const std::string& dir);
  static void prune(const std::string& dir, uint64_t newest, size_t keep);
};

}
