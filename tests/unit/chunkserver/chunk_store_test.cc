#include "chunkserver/chunk_store.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "common/crc32.h"
#include "common/ids.h"

namespace gfs {

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kChunk = 1 << 20;
constexpr uint64_t kBlock = 64 << 10;

class ChunkStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        (fs::temp_directory_path() / ("gfs-store-" + RandomHexId(6))).string();
    fs::create_directories(dir_);
  }
  void TearDown() override { fs::remove_all(dir_); }

  std::string ReadFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)),
                       std::istreambuf_iterator<char>());
  }

  void FlipByte(const std::string& path, uint64_t offset) {
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    f.seekg(static_cast<std::streamoff>(offset));
    char c = 0;
    f.read(&c, 1);
    c = static_cast<char>(c ^ 0x5a);
    f.seekp(static_cast<std::streamoff>(offset));
    f.write(&c, 1);
  }

  std::string Pattern(size_t n, char base) {
    std::string s(n, '\0');
    for (size_t i = 0; i < n; ++i) s[i] = static_cast<char>(base + (i % 23));
    return s;
  }

  std::string dir_;
};

uint32_t MetaEntry(const std::string& meta, size_t index) {
  const unsigned char* p =
      reinterpret_cast<const unsigned char*>(meta.data() + 8 + 4 * index);
  return p[0] | (p[1] << 8) | (p[2] << 16) |
         (static_cast<uint32_t>(p[3]) << 24);
}

uint64_t MetaVersion(const std::string& meta) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i)
    v |= static_cast<uint64_t>(static_cast<unsigned char>(meta[i])) << (8 * i);
  return v;
}

}  // namespace

TEST_F(ChunkStoreTest, CreateWriteReadPadLength) {
  ChunkStore store(dir_, kChunk, kBlock);
  ASSERT_EQ(store.Create(7, 3), rpc::OK);
  EXPECT_EQ(store.Create(7, 3), rpc::FAILED);
  EXPECT_TRUE(store.Contains(7));
  EXPECT_EQ(store.Version(7), 3u);
  std::string data = Pattern(100000, 'a');
  ASSERT_EQ(store.Write(7, 0, data), rpc::OK);
  uint64_t length = 0;
  ASSERT_EQ(store.Length(7, &length), rpc::OK);
  EXPECT_EQ(length, 100000u);
  std::string back;
  ASSERT_EQ(store.Read(7, 0, 100000, &back), rpc::OK);
  EXPECT_EQ(back, data);
  ASSERT_EQ(store.Read(7, 99990, 1000, &back), rpc::OK);
  EXPECT_EQ(back, data.substr(99990));
  ASSERT_EQ(store.Read(7, 100000, 10, &back), rpc::OK);
  EXPECT_TRUE(back.empty());
  EXPECT_EQ(store.Read(7, 100001, 10, &back), rpc::OUT_OF_RANGE);
  EXPECT_EQ(store.Write(7, kChunk - 5, "0123456789"), rpc::OUT_OF_RANGE);
  EXPECT_EQ(store.Read(99, 0, 1, &back), rpc::NO_SUCH_CHUNK);

  ASSERT_EQ(store.Pad(7, 100000), rpc::OK);
  ASSERT_EQ(store.Length(7, &length), rpc::OK);
  EXPECT_EQ(length, kChunk);
  ASSERT_EQ(store.Read(7, 99998, 6, &back), rpc::OK);
  EXPECT_EQ(back, data.substr(99998, 2) + std::string(4, '\0'));
  ASSERT_EQ(store.Read(7, kChunk - 3, 100, &back), rpc::OK);
  EXPECT_EQ(back, std::string(3, '\0'));

  auto listing = store.List();
  ASSERT_EQ(listing.size(), 1u);
  EXPECT_EQ(listing[0].handle, 7u);
  EXPECT_EQ(listing[0].version, 3u);
  EXPECT_EQ(listing[0].length, kChunk);

  ASSERT_EQ(store.SetVersion(7, 9), rpc::OK);
  EXPECT_EQ(store.Version(7), 9u);
  EXPECT_TRUE(store.Remove(7));
  EXPECT_FALSE(store.Contains(7));
  EXPECT_FALSE(fs::exists(store.ChunkPath(7)));
  EXPECT_FALSE(fs::exists(store.MetaPath(7)));
}

TEST_F(ChunkStoreTest, MetaLayoutMatchesTheFixedFormat) {
  ChunkStore store(dir_, kChunk, kBlock);
  ASSERT_EQ(store.Create(1, 5), rpc::OK);
  std::string data = Pattern(kBlock * 2 + 100, 'q');
  ASSERT_EQ(store.Write(1, 0, data), rpc::OK);
  std::string meta = ReadFile(store.MetaPath(1));
  ASSERT_EQ(meta.size(), 8 + 4 * 3);
  EXPECT_EQ(MetaVersion(meta), 5u);
  EXPECT_EQ(MetaEntry(meta, 0),
            Crc32(std::string_view(data).substr(0, kBlock)));
  EXPECT_EQ(MetaEntry(meta, 1),
            Crc32(std::string_view(data).substr(kBlock, kBlock)));
  EXPECT_EQ(MetaEntry(meta, 2),
            Crc32(std::string_view(data).substr(2 * kBlock)));

  ASSERT_EQ(store.SetVersion(1, 6), rpc::OK);
  meta = ReadFile(store.MetaPath(1));
  EXPECT_EQ(MetaVersion(meta), 6u);
  EXPECT_EQ(meta.size(), 8 + 4 * 3);
}

TEST_F(ChunkStoreTest, OverlappingWriteRecomputesOnlyTouchedBlocks) {
  ChunkStore store(dir_, kChunk, kBlock);
  ASSERT_EQ(store.Create(2, 1), rpc::OK);
  std::string data = Pattern(kBlock * 4, 'a');
  ASSERT_EQ(store.Write(2, 0, data), rpc::OK);
  std::string patch = Pattern(1000, 'z');
  ASSERT_EQ(store.Write(2, kBlock * 2 - 500, patch), rpc::OK);
  data.replace(kBlock * 2 - 500, 1000, patch);
  std::string meta = ReadFile(store.MetaPath(2));
  for (size_t b = 0; b < 4; ++b) {
    EXPECT_EQ(MetaEntry(meta, b),
              Crc32(std::string_view(data).substr(b * kBlock, kBlock)))
        << "block " << b;
  }
  std::string back;
  ASSERT_EQ(store.Read(2, 0, data.size(), &back), rpc::OK);
  EXPECT_EQ(back, data);
}

TEST_F(ChunkStoreTest, WriteBeyondLengthZeroFillsTheGap) {
  ChunkStore store(dir_, kChunk, kBlock);
  ASSERT_EQ(store.Create(3, 1), rpc::OK);
  ASSERT_EQ(store.Write(3, 0, "head"), rpc::OK);
  ASSERT_EQ(store.Write(3, kBlock + 10, "tail"), rpc::OK);
  std::string back;
  ASSERT_EQ(store.Read(3, 0, kBlock + 14, &back), rpc::OK);
  EXPECT_EQ(back.substr(0, 4), "head");
  EXPECT_EQ(back.substr(4, kBlock + 6), std::string(kBlock + 6, '\0'));
  EXPECT_EQ(back.substr(kBlock + 10), "tail");
}

TEST_F(ChunkStoreTest, CorruptionOfTheChunkFileIsDetectedOnRead) {
  ChunkStore store(dir_, kChunk, kBlock);
  ASSERT_EQ(store.Create(4, 1), rpc::OK);
  std::string data = Pattern(kBlock * 3, 'm');
  ASSERT_EQ(store.Write(4, 0, data), rpc::OK);
  FlipByte(store.ChunkPath(4), kBlock + 17);
  std::string back;
  EXPECT_EQ(store.Read(4, 0, kBlock, &back), rpc::OK);
  EXPECT_EQ(store.Read(4, kBlock * 2, kBlock, &back), rpc::OK);
  EXPECT_EQ(store.Read(4, kBlock - 1, 2, &back), rpc::CHECKSUM_MISMATCH);
  EXPECT_EQ(store.CorruptHandles(), std::vector<uint64_t>{4});
  store.ClearCorrupt({4});
  EXPECT_TRUE(store.CorruptHandles().empty());
}

TEST_F(ChunkStoreTest, CopyDuplicatesDataAndChecksums) {
  ChunkStore store(dir_, kChunk, kBlock);
  ASSERT_EQ(store.Create(5, 2), rpc::OK);
  std::string data = Pattern(kBlock + 333, 'c');
  ASSERT_EQ(store.Write(5, 0, data), rpc::OK);
  EXPECT_EQ(store.CreateCopy(6, 3, 42), rpc::NO_SUCH_CHUNK);
  ASSERT_EQ(store.CreateCopy(6, 3, 5), rpc::OK);
  EXPECT_EQ(store.Version(6), 3u);
  std::string back;
  ASSERT_EQ(store.Read(6, 0, data.size(), &back), rpc::OK);
  EXPECT_EQ(back, data);
  EXPECT_EQ(ReadFile(store.MetaPath(6)).substr(8),
            ReadFile(store.MetaPath(5)).substr(8));
  ASSERT_EQ(store.Write(6, 0, "changed"), rpc::OK);
  ASSERT_EQ(store.Read(5, 0, 7, &back), rpc::OK);
  EXPECT_EQ(back, data.substr(0, 7));
}

TEST_F(ChunkStoreTest, ScanReloadsAndRepairs) {
  std::string data = Pattern(kBlock * 2 + 5, 'r');
  {
    ChunkStore store(dir_, kChunk, kBlock);
    ASSERT_EQ(store.Create(10, 4), rpc::OK);
    ASSERT_EQ(store.Write(10, 0, data), rpc::OK);
    ASSERT_EQ(store.Create(11, 1), rpc::OK);
    ASSERT_EQ(store.Write(11, 0, data), rpc::OK);
    ASSERT_EQ(store.Create(12, 1), rpc::OK);
    ASSERT_EQ(store.Write(12, 0, data), rpc::OK);
  }
  fs::remove(dir_ + "/" + HandleToHex(11) + ".meta");
  fs::remove(dir_ + "/" + HandleToHex(12) + ".chunk");
  fs::resize_file(dir_ + "/" + HandleToHex(10) + ".meta", 8 + 4 * 2);
  std::ofstream(dir_ + "/notachunk.txt") << "ignored";

  ChunkStore store(dir_, kChunk, kBlock);
  store.Scan();
  EXPECT_TRUE(store.Contains(10));
  EXPECT_FALSE(store.Contains(11));
  EXPECT_FALSE(store.Contains(12));
  EXPECT_FALSE(fs::exists(dir_ + "/" + HandleToHex(12) + ".meta"));
  EXPECT_TRUE(fs::exists(dir_ + "/" + HandleToHex(11) + ".chunk"));
  EXPECT_EQ(store.Version(10), 4u);
  uint64_t length = 0;
  ASSERT_EQ(store.Length(10, &length), rpc::OK);
  EXPECT_EQ(length, data.size());
  std::string back;
  EXPECT_EQ(store.Read(10, 0, kBlock * 2, &back), rpc::OK);
  EXPECT_EQ(back, data.substr(0, kBlock * 2));
  EXPECT_EQ(store.Read(10, kBlock * 2, 5, &back), rpc::CHECKSUM_MISMATCH);
  ASSERT_EQ(store.Write(10, kBlock * 2, "fixed"), rpc::OK);
  EXPECT_EQ(store.Read(10, kBlock * 2, 5, &back), rpc::OK);
  EXPECT_EQ(back, "fixed");
}

TEST_F(ChunkStoreTest, ChunkserverIdIsStable) {
  std::string first = LoadOrCreateChunkserverId(dir_);
  EXPECT_EQ(first.size(), 32u);
  EXPECT_EQ(LoadOrCreateChunkserverId(dir_), first);
}

}  // namespace gfs
