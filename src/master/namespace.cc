#include "master/namespace.h"

#include "common/paths.h"

namespace gfs {

FileMeta* Namespace::find(const std::string& path) {
  auto it = files_.find(path);
  return it == files_.end() ? nullptr : &it->second;
}

const FileMeta* Namespace::find(const std::string& path) const {
  auto it = files_.find(path);
  return it == files_.end() ? nullptr : &it->second;
}

bool Namespace::exists(const std::string& path) const { return files_.count(path) > 0; }

bool Namespace::isDirectory(const std::string& path) const {
  if (path == "/") return true;
  std::string prefix = childPrefix(path);
  auto it = files_.lower_bound(prefix);
  return it != files_.end() && it->first.compare(0, prefix.size(), prefix) == 0;
}

bool Namespace::hasFileAncestor(const std::string& path) const {
  for (const auto& ancestor : ancestorsOf(path)) {
    if (ancestor != "/" && exists(ancestor)) return true;
  }
  return false;
}

bool Namespace::insert(const std::string& path, FileMeta meta) {
  return files_.emplace(path, std::move(meta)).second;
}

bool Namespace::erase(const std::string& path) { return files_.erase(path) > 0; }

std::vector<std::pair<std::string, FileMeta>> Namespace::subtree(const std::string& root, bool skip_hidden) const {
  std::vector<std::pair<std::string, FileMeta>> out;
  if (const FileMeta* self = find(root)) {
    if (!skip_hidden || !isHiddenPath(root)) out.emplace_back(root, *self);
  }
  std::string prefix = childPrefix(root);
  for (auto it = files_.lower_bound(prefix); it != files_.end(); ++it) {
    if (it->first.compare(0, prefix.size(), prefix) != 0) break;
    if (skip_hidden && isHiddenPath(it->first)) continue;
    out.emplace_back(it->first, it->second);
  }
  return out;
}

std::vector<ListEntry> Namespace::list(const std::string& directory, bool include_hidden) const {
  std::vector<ListEntry> out;
  std::string prefix = childPrefix(directory);
  std::string last_directory;
  for (auto it = files_.lower_bound(prefix); it != files_.end(); ++it) {
    if (it->first.compare(0, prefix.size(), prefix) != 0) break;
    std::string rest = it->first.substr(prefix.size());
    size_t slash = rest.find('/');
    if (slash == std::string::npos) {
      if (!include_hidden && isHiddenPath(it->first)) continue;
      out.push_back({rest, false});
      continue;
    }
    std::string name = rest.substr(0, slash);
    if (name == last_directory) continue;
    last_directory = name;
    out.push_back({name, true});
  }
  return out;
}

void Namespace::renameSubtree(const std::string& source, const std::string& target) {
  std::vector<std::pair<std::string, FileMeta>> moved;
  auto self = files_.find(source);
  if (self != files_.end()) {
    moved.emplace_back(target, std::move(self->second));
    files_.erase(self);
  }
  std::string prefix = childPrefix(source);
  auto it = files_.lower_bound(prefix);
  while (it != files_.end() && it->first.compare(0, prefix.size(), prefix) == 0) {
    moved.emplace_back(childPrefix(target) + it->first.substr(prefix.size()), std::move(it->second));
    it = files_.erase(it);
  }
  for (auto& [path, meta] : moved) files_[path] = std::move(meta);
}

}
