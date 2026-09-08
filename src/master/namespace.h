#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace gfs {

struct FileMeta {
  std::vector<uint64_t> chunks;
};

struct ListEntry {
  std::string name;
  bool is_directory = false;
};

class Namespace {
 public:
  FileMeta* find(const std::string& path);
  const FileMeta* find(const std::string& path) const;
  bool exists(const std::string& path) const;
  bool isDirectory(const std::string& path) const;
  bool hasFileAncestor(const std::string& path) const;
  bool insert(const std::string& path, FileMeta meta);
  bool erase(const std::string& path);

  std::vector<std::pair<std::string, FileMeta>> subtree(const std::string& root, bool skip_hidden) const;
  std::vector<ListEntry> list(const std::string& directory, bool include_hidden) const;
  void renameSubtree(const std::string& source, const std::string& target);

  template <typename F>
  void forEach(F f) const {
    for (const auto& [path, meta] : files_) f(path, meta);
  }

  size_t size() const { return files_.size(); }
  void clear() { files_.clear(); }

 private:
  std::map<std::string, FileMeta> files_;
};

}
