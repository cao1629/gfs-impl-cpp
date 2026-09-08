#pragma once

#include <chrono>
#include <cstdint>

namespace gfs {

using SteadyClock = std::chrono::steady_clock;
using TimePoint = SteadyClock::time_point;

inline TimePoint now() { return SteadyClock::now(); }

inline int64_t unixSeconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

}
