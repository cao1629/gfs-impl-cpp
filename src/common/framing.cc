#include "common/framing.h"

#include <cstring>

#include "common/crc32.h"

namespace gfs {

namespace {

void PutU32(std::string* out, uint32_t v) {
  char buf[4];
  for (int i = 0; i < 4; ++i) buf[i] = static_cast<char>((v >> (8 * i)) & 0xff);
  out->append(buf, 4);
}

uint32_t GetU32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i)
    v |= static_cast<uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

}  // namespace

std::string EncodeRecord(std::string_view payload) {
  std::string out;
  out.reserve(kFrameHeaderSize + payload.size());
  PutU32(&out, static_cast<uint32_t>(payload.size()));
  PutU32(&out, Crc32(payload));
  out.append(payload.data(), payload.size());
  return out;
}

DecodedRecords DecodeRecords(std::string_view bytes) {
  DecodedRecords result;
  size_t pos = 0;
  while (pos + kFrameHeaderSize <= bytes.size()) {
    uint32_t len = GetU32(bytes.data() + pos);
    uint32_t crc = GetU32(bytes.data() + pos + 4);
    if (pos + kFrameHeaderSize + len > bytes.size()) {
      result.torn_tail = true;
      break;
    }
    std::string_view payload = bytes.substr(pos + kFrameHeaderSize, len);
    if (Crc32(payload) != crc) {
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

}  // namespace gfs
