#include <gtest/gtest.h>

#include "common/paths.h"

namespace gfs {

TEST(Paths, ValidatesSyntax) {
  EXPECT_TRUE(isValidPath("/"));
  EXPECT_TRUE(isValidPath("/a"));
  EXPECT_TRUE(isValidPath("/a/b.c/d"));
  EXPECT_FALSE(isValidPath(""));
  EXPECT_FALSE(isValidPath("a"));
  EXPECT_FALSE(isValidPath("/a/"));
  EXPECT_FALSE(isValidPath("/a//b"));
  EXPECT_FALSE(isValidPath("/a/./b"));
  EXPECT_FALSE(isValidPath("/a/../b"));
}

TEST(Paths, ParentsAndAncestors) {
  EXPECT_EQ(parentOf("/a/b/c"), "/a/b");
  EXPECT_EQ(parentOf("/a"), "/");
  EXPECT_EQ(parentOf("/"), "/");
  EXPECT_EQ(baseName("/a/b/c"), "c");
  std::vector<std::string> expected = {"/", "/a", "/a/b"};
  EXPECT_EQ(ancestorsOf("/a/b/c"), expected);
  EXPECT_TRUE(ancestorsOf("/").empty());
  EXPECT_TRUE(isAncestorOrSelf("/a", "/a/b"));
  EXPECT_TRUE(isAncestorOrSelf("/", "/a"));
  EXPECT_FALSE(isAncestorOrSelf("/a", "/ab"));
  EXPECT_EQ(childPrefix("/"), "/");
  EXPECT_EQ(childPrefix("/a"), "/a/");
}

TEST(Paths, HiddenNames) {
  std::string hidden = hiddenNameFor("/home/user/file", 1725580800);
  EXPECT_EQ(hidden, "/home/user/.deleted.1725580800.file");
  EXPECT_TRUE(isHiddenPath(hidden));
  EXPECT_FALSE(isHiddenPath("/home/user/file"));
  int64_t ts = 0;
  std::string original;
  ASSERT_TRUE(parseHiddenName(hidden, &ts, &original));
  EXPECT_EQ(ts, 1725580800);
  EXPECT_EQ(original, "/home/user/file");
  EXPECT_FALSE(parseHiddenName("/home/user/.deleted.x.file", &ts, &original));
  EXPECT_EQ(hiddenNameFor("/top", 7), "/.deleted.7.top");
}

}
