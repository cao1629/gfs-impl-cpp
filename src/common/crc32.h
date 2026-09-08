#pragma once

#include <cstdint>
#include <string_view>

namespace gfs {

uint32_t crc32(std::string_view bytes);
uint32_t crc32(uint32_t seed, std::string_view bytes);

}
