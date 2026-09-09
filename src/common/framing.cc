#include "common/framing.h"

#include <cstring>

#include "common/crc32.h"

namespace gfs {

namespace {

void putU32(std::string* out, uint32_t v) {
  char buf[4];
  for (int i = 0; i < 4; ++i) buf[i] = static_cast<char>((v >> (8 * i)) & 0xff);
  out->append(buf, 4);
}

uint32_t getU32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

}

std::string encodeRecord(std::string_view payload) {
  std::string out;
  out.reserve(kFrameHeaderSize + payload.size());
  putU32(&out, static_cast<uint32_t>(payload.size()));
  putU32(&out, crc32(payload));
  out.append(payload.data(), payload.size());
  return out;
}

DecodedRecords decodeRecords(std::string_view bytes) {
  DecodedRecords result;
  size_t pos = 0;
  while (pos + kFrameHeaderSize <= bytes.size()) {
    uint32_t len = getU32(bytes.data() + pos);
    uint32_t crc = getU32(bytes.data() + pos + 4);
    if (pos + kFrameHeaderSize + len > bytes.size()) {
      result.torn_tail = true;
      break;
    }
    std::string_view payload = bytes.substr(pos + kFrameHeaderSize, len);
    if (crc32(payload) != crc) {
      result.torn_tail = true;
      break;
    }
    result.payloads.emplace_back(payload);
    pos += kFrameHeaderSize + len;
  }
  if (pos < bytes.size() && !result.torn_tail) result.torn_tail = true;
  result.consumed = pos;
  return result;
}

}
