#include "common/paths.h"

#include <charconv>

namespace gfs {

bool isValidPath(std::string_view path) {
  if (path.empty() || path[0] != '/') return false;
  if (path == "/") return true;
  if (path.back() == '/') return false;
  size_t start = 1;
  while (start <= path.size()) {
    size_t end = path.find('/', start);
    if (end == std::string_view::npos) end = path.size();
    std::string_view component = path.substr(start, end - start);
    if (component.empty() || component == "." || component == "..") return false;
    start = end + 1;
  }
  return true;
}

std::string parentOf(std::string_view path) {
  if (path == "/") return "/";
  size_t slash = path.rfind('/');
  if (slash == 0) return "/";
  return std::string(path.substr(0, slash));
}

std::string baseName(std::string_view path) {
  size_t slash = path.rfind('/');
  return std::string(path.substr(slash + 1));
}

std::vector<std::string> ancestorsOf(std::string_view path) {
  std::vector<std::string> out;
  if (path == "/") return out;
  out.emplace_back("/");
  for (size_t i = 1; i < path.size(); ++i) {
    if (path[i] == '/') out.emplace_back(path.substr(0, i));
  }
  return out;
}

bool isAncestorOrSelf(std::string_view ancestor, std::string_view path) {
  if (ancestor == path) return true;
  if (ancestor == "/") return true;
  return path.size() > ancestor.size() && path.substr(0, ancestor.size()) == ancestor && path[ancestor.size()] == '/';
}

std::string childPrefix(std::string_view directory) {
  if (directory == "/") return "/";
  return std::string(directory) + "/";
}

bool isHiddenPath(std::string_view path) {
  std::string name = baseName(path);
  return name.size() > kHiddenPrefix.size() && std::string_view(name).substr(0, kHiddenPrefix.size()) == kHiddenPrefix;
}

std::string hiddenNameFor(std::string_view path, int64_t unix_seconds) {
  std::string parent = parentOf(path);
  std::string name = baseName(path);
  return childPrefix(parent) + std::string(kHiddenPrefix) + std::to_string(unix_seconds) + "." + name;
}

bool parseHiddenName(std::string_view path, int64_t* unix_seconds, std::string* original) {
  if (!isHiddenPath(path)) return false;
  std::string name = baseName(path);
  std::string_view rest = std::string_view(name).substr(kHiddenPrefix.size());
  size_t dot = rest.find('.');
  if (dot == std::string_view::npos || dot == 0 || dot + 1 >= rest.size()) return false;
  int64_t ts = 0;
  auto [ptr, ec] = std::from_chars(rest.data(), rest.data() + dot, ts);
  if (ec != std::errc() || ptr != rest.data() + dot) return false;
  if (unix_seconds) *unix_seconds = ts;
  if (original) *original = childPrefix(parentOf(path)) + std::string(rest.substr(dot + 1));
  return true;
}

}
