#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace gfs {

bool isValidPath(std::string_view path);
std::string parentOf(std::string_view path);
std::string baseName(std::string_view path);
std::vector<std::string> ancestorsOf(std::string_view path);
bool isAncestorOrSelf(std::string_view ancestor, std::string_view path);
std::string childPrefix(std::string_view directory);

constexpr std::string_view kHiddenPrefix = ".deleted.";

bool isHiddenPath(std::string_view path);
std::string hiddenNameFor(std::string_view path, int64_t unix_seconds);
bool parseHiddenName(std::string_view path, int64_t* unix_seconds, std::string* original);

}
