#include "common/crc32.h"

#include <zlib.h>

namespace gfs {

uint32_t Crc32(std::string_view bytes) { return Crc32(0, bytes); }

uint32_t Crc32(uint32_t seed, std::string_view bytes) {
  return static_cast<uint32_t>(
      ::crc32(seed, reinterpret_cast<const Bytef*>(bytes.data()),
              static_cast<uInt>(bytes.size())));
}

}  // namespace gfs
