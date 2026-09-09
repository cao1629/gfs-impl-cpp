#include <gtest/gtest.h>

#include "master/namespace.h"

namespace gfs {

namespace {

std::vector<std::string> namesOf(const std::vector<ListEntry>& entries) {
  std::vector<std::string> out;
  for (const auto& e : entries) out.push_back(e.name + (e.is_directory ? "/" : ""));
  return out;
}

}

TEST(Namespace, ListsImmediateChildrenAndImpliedDirectories) {
  Namespace ns;
  ns.insert("/a/b", {});
  ns.insert("/a/c/d", {});
  ns.insert("/a/c/e", {});
  ns.insert("/a/.deleted.5.gone", {});
  ns.insert("/z", {});
  EXPECT_EQ(namesOf(ns.list("/", false)), (std::vector<std::string>{"a/", "z"}));
  EXPECT_EQ(namesOf(ns.list("/a", false)), (std::vector<std::string>{"b", "c/"}));
  EXPECT_EQ(namesOf(ns.list("/a", true)), (std::vector<std::string>{".deleted.5.gone", "b", "c/"}));
  EXPECT_TRUE(ns.list("/nope", false).empty());
  EXPECT_TRUE(ns.isDirectory("/a/c"));
  EXPECT_TRUE(ns.isDirectory("/"));
  EXPECT_FALSE(ns.isDirectory("/a/b"));
  EXPECT_FALSE(ns.isDirectory("/a/cc"));
  EXPECT_TRUE(ns.hasFileAncestor("/a/b/x"));
  EXPECT_FALSE(ns.hasFileAncestor("/a/c/x"));
}

TEST(Namespace, SubtreeAndRename) {
  Namespace ns;
  ns.insert("/a", {{1}});
  ns.insert("/a/b", {{2}});
  ns.insert("/ab", {{3}});
  auto only_file = ns.subtree("/ab", false);
  ASSERT_EQ(only_file.size(), 1u);
  EXPECT_EQ(only_file[0].first, "/ab");
  ns.erase("/a");
  ns.insert("/d/x", {{4}});
  ns.insert("/d/y/z", {{5}});
  ns.insert("/d/.deleted.1.h", {{6}});
  auto tree = ns.subtree("/d", true);
  ASSERT_EQ(tree.size(), 2u);
  EXPECT_EQ(tree[0].first, "/d/x");
  EXPECT_EQ(tree[1].first, "/d/y/z");
  EXPECT_EQ(ns.subtree("/d", false).size(), 3u);
  ns.renameSubtree("/d", "/e/f");
  EXPECT_FALSE(ns.isDirectory("/d"));
  ASSERT_NE(ns.find("/e/f/y/z"), nullptr);
  EXPECT_EQ(ns.find("/e/f/y/z")->chunks, std::vector<uint64_t>{5});
  ASSERT_NE(ns.find("/e/f/.deleted.1.h"), nullptr);
  ns.renameSubtree("/e/f/x", "/top");
  ASSERT_NE(ns.find("/top"), nullptr);
  EXPECT_EQ(ns.find("/top")->chunks, std::vector<uint64_t>{4});
  EXPECT_EQ(ns.find("/e/f/x"), nullptr);
}

}
