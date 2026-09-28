#pragma once

#include <cstdint>
#include <string_view>

namespace gfs {

uint32_t Crc32(std::string_view bytes);
uint32_t Crc32(uint32_t seed, std::string_view bytes);

}  // namespace gfs
