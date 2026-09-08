#include "chunkserver/lease_table.h"

namespace gfs {

LeaseTable::LeaseTable(Millis skew_margin) : skew_margin_(skew_margin) {}

void LeaseTable::grant(uint64_t handle, Millis lease, std::vector<rpc::Replica> secondaries) {
  std::lock_guard<std::mutex> lock(mutex_);
  Slot& slot = slots_[handle];
  slot.held = true;
  slot.expiry = now() + lease - skew_margin_;
  slot.secondaries = std::move(secondaries);
}

bool LeaseTable::extend(uint64_t handle, Millis lease) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = slots_.find(handle);
  if (it == slots_.end() || !it->second.held) return false;
  it->second.expiry = now() + lease - skew_margin_;
  return true;
}

void LeaseTable::revoke(uint64_t handle) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = slots_.find(handle);
  if (it == slots_.end()) return;
  it->second.held = false;
  it->second.secondaries.clear();
}

LeaseCheck LeaseTable::check(uint64_t handle, LeaseInfo* info) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = slots_.find(handle);
  if (it == slots_.end() || !it->second.held) return LeaseCheck::kNotHeld;
  if (now() > it->second.expiry) return LeaseCheck::kExpired;
  if (info) {
    info->expiry = it->second.expiry;
    info->secondaries = it->second.secondaries;
  }
  return LeaseCheck::kPrimary;
}

uint64_t LeaseTable::nextSerial(uint64_t handle) {
  std::lock_guard<std::mutex> lock(mutex_);
  return slots_[handle].next_serial++;
}

std::vector<uint64_t> LeaseTable::heldHandles() {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<uint64_t> out;
  TimePoint t = now();
  for (const auto& [handle, slot] : slots_) {
    if (slot.held && t <= slot.expiry) out.push_back(handle);
  }
  return out;
}

}
