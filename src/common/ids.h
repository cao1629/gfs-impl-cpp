#pragma once

#include <cstdint>
#include <string>

namespace gfs {

std::string RandomHexId(size_t bytes = 16);
std::string HandleToHex(uint64_t handle);
bool HexToHandle(const std::string& text, uint64_t* handle);

}  // namespace gfs
