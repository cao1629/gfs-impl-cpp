#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace gfs {

bool IsValidPath(std::string_view path);
std::string ParentOf(std::string_view path);
std::string BaseName(std::string_view path);
std::vector<std::string> AncestorsOf(std::string_view path);
bool IsAncestorOrSelf(std::string_view ancestor, std::string_view path);
std::string ChildPrefix(std::string_view directory);

constexpr std::string_view kHiddenPrefix = ".deleted.";

bool IsHiddenPath(std::string_view path);
std::string HiddenNameFor(std::string_view path, int64_t unix_seconds);
bool ParseHiddenName(std::string_view path, int64_t* unix_seconds,
                     std::string* original);

}  // namespace gfs
