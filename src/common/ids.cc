#include "common/ids.h"

#include <charconv>
#include <random>

namespace gfs {

std::string randomHexId(size_t bytes) {
  static const char* digits = "0123456789abcdef";
  std::random_device rd;
  std::string out;
  out.reserve(bytes * 2);
  for (size_t i = 0; i < bytes; ++i) {
    unsigned byte = rd() & 0xff;
    out.push_back(digits[byte >> 4]);
    out.push_back(digits[byte & 0xf]);
  }
  return out;
}

std::string handleToHex(uint64_t handle) {
  static const char* digits = "0123456789abcdef";
  std::string out(16, '0');
  for (int i = 15; i >= 0; --i) {
    out[i] = digits[handle & 0xf];
    handle >>= 4;
  }
  return out;
}

bool hexToHandle(const std::string& text, uint64_t* handle) {
  if (text.size() != 16) return false;
  uint64_t value = 0;
  auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value, 16);
  if (ec != std::errc() || ptr != text.data() + text.size()) return false;
  *handle = value;
  return true;
}

}
