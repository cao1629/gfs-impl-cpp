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
  FileMeta* Find(const std::string& path);
  const FileMeta* Find(const std::string& path) const;
  bool Exists(const std::string& path) const;
  bool IsDirectory(const std::string& path) const;
  bool HasFileAncestor(const std::string& path) const;
  bool Insert(const std::string& path, FileMeta meta);
  bool Erase(const std::string& path);

  std::vector<std::pair<std::string, FileMeta>> Subtree(const std::string& root,
                                                        bool skip_hidden) const;
  std::vector<ListEntry> List(const std::string& directory,
                              bool include_hidden) const;
  void RenameSubtree(const std::string& source, const std::string& target);

  template <typename F>
  void ForEach(F f) const {
    for (const auto& [path, meta] : files_) f(path, meta);
  }

  size_t Size() const { return files_.size(); }
  void Clear() { files_.clear(); }

 private:
  std::map<std::string, FileMeta> files_;
};

}  // namespace gfs
