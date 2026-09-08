#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/clock.h"
#include "common/config.h"
#include "gfs.pb.h"

namespace gfs {

struct LeaseInfo {
  TimePoint expiry;
  std::vector<rpc::Replica> secondaries;
};

enum class LeaseCheck { kPrimary, kExpired, kNotHeld };

class LeaseTable {
 public:
  explicit LeaseTable(Millis skew_margin);

  void grant(uint64_t handle, Millis lease, std::vector<rpc::Replica> secondaries);
  bool extend(uint64_t handle, Millis lease);
  void revoke(uint64_t handle);
  LeaseCheck check(uint64_t handle, LeaseInfo* info);
  uint64_t nextSerial(uint64_t handle);
  std::vector<uint64_t> heldHandles();

 private:
  struct Slot {
    bool held = false;
    TimePoint expiry;
    std::vector<rpc::Replica> secondaries;
    uint64_t next_serial = 1;
  };

  std::mutex mutex_;
  Millis skew_margin_;
  std::unordered_map<uint64_t, Slot> slots_;
};

}
