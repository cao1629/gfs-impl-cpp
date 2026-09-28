#include "common/paths.h"

#include <gtest/gtest.h>

namespace gfs {

TEST(Paths, ValidatesSyntax) {
  EXPECT_TRUE(IsValidPath("/"));
  EXPECT_TRUE(IsValidPath("/a"));
  EXPECT_TRUE(IsValidPath("/a/b.c/d"));
  EXPECT_FALSE(IsValidPath(""));
  EXPECT_FALSE(IsValidPath("a"));
  EXPECT_FALSE(IsValidPath("/a/"));
  EXPECT_FALSE(IsValidPath("/a//b"));
  EXPECT_FALSE(IsValidPath("/a/./b"));
  EXPECT_FALSE(IsValidPath("/a/../b"));
}

TEST(Paths, ParentsAndAncestors) {
  EXPECT_EQ(ParentOf("/a/b/c"), "/a/b");
  EXPECT_EQ(ParentOf("/a"), "/");
  EXPECT_EQ(ParentOf("/"), "/");
  EXPECT_EQ(BaseName("/a/b/c"), "c");
  std::vector<std::string> expected = {"/", "/a", "/a/b"};
  EXPECT_EQ(AncestorsOf("/a/b/c"), expected);
  EXPECT_TRUE(AncestorsOf("/").empty());
  EXPECT_TRUE(IsAncestorOrSelf("/a", "/a/b"));
  EXPECT_TRUE(IsAncestorOrSelf("/", "/a"));
  EXPECT_FALSE(IsAncestorOrSelf("/a", "/ab"));
  EXPECT_EQ(ChildPrefix("/"), "/");
  EXPECT_EQ(ChildPrefix("/a"), "/a/");
}

TEST(Paths, HiddenNames) {
  std::string hidden = HiddenNameFor("/home/user/file", 1725580800);
  EXPECT_EQ(hidden, "/home/user/.deleted.1725580800.file");
  EXPECT_TRUE(IsHiddenPath(hidden));
  EXPECT_FALSE(IsHiddenPath("/home/user/file"));
  int64_t ts = 0;
  std::string original;
  ASSERT_TRUE(ParseHiddenName(hidden, &ts, &original));
  EXPECT_EQ(ts, 1725580800);
  EXPECT_EQ(original, "/home/user/file");
  EXPECT_FALSE(ParseHiddenName("/home/user/.deleted.x.file", &ts, &original));
  EXPECT_EQ(HiddenNameFor("/top", 7), "/.deleted.7.top");
}

}  // namespace gfs
