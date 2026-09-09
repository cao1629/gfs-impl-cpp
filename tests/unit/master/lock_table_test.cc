#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "master/lock_table.h"

namespace gfs {

TEST(LockTable, NormalizesIntoGlobalOrderAndDedupes) {
  auto ordered = LockTable::normalize({{"/home/user", LockMode::kRead}, {"/", LockMode::kRead}, {"/home", LockMode::kRead},
                                       {"/home/user", LockMode::kWrite}, {"/b", LockMode::kRead}, {"/a", LockMode::kWrite}});
  ASSERT_EQ(ordered.size(), 5u);
  EXPECT_EQ(ordered[0].path, "/");
  EXPECT_EQ(ordered[1].path, "/a");
  EXPECT_EQ(ordered[2].path, "/b");
  EXPECT_EQ(ordered[3].path, "/home");
  EXPECT_EQ(ordered[4].path, "/home/user");
  EXPECT_EQ(ordered[4].mode, LockMode::kWrite);
  auto for_path = LockTable::forPath("/x/y/z", LockMode::kWrite);
  ASSERT_EQ(for_path.size(), 4u);
  EXPECT_EQ(for_path[0].path, "/");
  EXPECT_EQ(for_path[3].mode, LockMode::kWrite);
}

TEST(LockTable, ReadersShareWritersExcludeAndEntriesAreReclaimed) {
  LockTable table;
  LockSet first = table.acquire(LockTable::forPath("/d/f", LockMode::kRead));
  LockSet second = table.acquire(LockTable::forPath("/d/g", LockMode::kRead));
  std::atomic<bool> writer_done{false};
  std::thread writer([&] {
    LockSet w = table.acquire(LockTable::forPath("/d", LockMode::kWrite));
    writer_done = true;
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(writer_done.load());
  first.release();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(writer_done.load());
  second.release();
  writer.join();
  EXPECT_TRUE(writer_done.load());
  EXPECT_EQ(table.activeEntries(), 0u);
}

TEST(LockTable, LockSetCanBeReleasedFromAnotherThread) {
  LockTable table;
  LockSet held;
  std::thread taker([&] { held = table.acquire(LockTable::forPath("/p", LockMode::kWrite)); });
  taker.join();
  std::atomic<bool> got{false};
  std::thread waiter([&] {
    LockSet w = table.acquire(LockTable::forPath("/p", LockMode::kRead));
    got = true;
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(got.load());
  std::thread releaser([&] { held.release(); });
  releaser.join();
  waiter.join();
  EXPECT_TRUE(got.load());
  EXPECT_EQ(table.activeEntries(), 0u);
}

}
