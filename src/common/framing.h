#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace gfs {

constexpr size_t kFrameHeaderSize = 8;

std::string encodeRecord(std::string_view payload);

struct DecodedRecords {
  std::vector<std::string> payloads;
  size_t consumed = 0;
  bool torn_tail = false;
};

DecodedRecords decodeRecords(std::string_view bytes);

}
