#pragma once

#include <cstdint>
#include <string>

namespace gfs {

std::string randomHexId(size_t bytes = 16);
std::string handleToHex(uint64_t handle);
bool hexToHandle(const std::string& text, uint64_t* handle);

}
