#include "master/namespace.h"

#include <gtest/gtest.h>

namespace gfs {

namespace {

std::vector<std::string> NamesOf(const std::vector<ListEntry>& entries) {
  std::vector<std::string> out;
  for (const auto& e : entries)
    out.push_back(e.name + (e.is_directory ? "/" : ""));
  return out;
}

}  // namespace

TEST(Namespace, ListsImmediateChildrenAndImpliedDirectories) {
  Namespace ns;
  ns.Insert("/a/b", {});
  ns.Insert("/a/c/d", {});
  ns.Insert("/a/c/e", {});
  ns.Insert("/a/.deleted.5.gone", {});
  ns.Insert("/z", {});
  EXPECT_EQ(NamesOf(ns.List("/", false)),
            (std::vector<std::string>{"a/", "z"}));
  EXPECT_EQ(NamesOf(ns.List("/a", false)),
            (std::vector<std::string>{"b", "c/"}));
  EXPECT_EQ(NamesOf(ns.List("/a", true)),
            (std::vector<std::string>{".deleted.5.gone", "b", "c/"}));
  EXPECT_TRUE(ns.List("/nope", false).empty());
  EXPECT_TRUE(ns.IsDirectory("/a/c"));
  EXPECT_TRUE(ns.IsDirectory("/"));
  EXPECT_FALSE(ns.IsDirectory("/a/b"));
  EXPECT_FALSE(ns.IsDirectory("/a/cc"));
  EXPECT_TRUE(ns.HasFileAncestor("/a/b/x"));
  EXPECT_FALSE(ns.HasFileAncestor("/a/c/x"));
}

TEST(Namespace, SubtreeAndRename) {
  Namespace ns;
  ns.Insert("/a", {{1}});
  ns.Insert("/a/b", {{2}});
  ns.Insert("/ab", {{3}});
  auto only_file = ns.Subtree("/ab", false);
  ASSERT_EQ(only_file.size(), 1u);
  EXPECT_EQ(only_file[0].first, "/ab");
  ns.Erase("/a");
  ns.Insert("/d/x", {{4}});
  ns.Insert("/d/y/z", {{5}});
  ns.Insert("/d/.deleted.1.h", {{6}});
  auto tree = ns.Subtree("/d", true);
  ASSERT_EQ(tree.size(), 2u);
  EXPECT_EQ(tree[0].first, "/d/x");
  EXPECT_EQ(tree[1].first, "/d/y/z");
  EXPECT_EQ(ns.Subtree("/d", false).size(), 3u);
  ns.RenameSubtree("/d", "/e/f");
  EXPECT_FALSE(ns.IsDirectory("/d"));
  ASSERT_NE(ns.Find("/e/f/y/z"), nullptr);
  EXPECT_EQ(ns.Find("/e/f/y/z")->chunks, std::vector<uint64_t>{5});
  ASSERT_NE(ns.Find("/e/f/.deleted.1.h"), nullptr);
  ns.RenameSubtree("/e/f/x", "/top");
  ASSERT_NE(ns.Find("/top"), nullptr);
  EXPECT_EQ(ns.Find("/top")->chunks, std::vector<uint64_t>{4});
  EXPECT_EQ(ns.Find("/e/f/x"), nullptr);
}

}  // namespace gfs
