#include "chunkserver/chunk_store.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>

#include "common/crc32.h"
#include "common/ids.h"
#include "common/logging.h"

namespace gfs {

namespace fs = std::filesystem;

struct ChunkEntry {
  std::mutex mutation_mutex;
  std::mutex state_mutex;
  int chunk_fd = -1;
  int meta_fd = -1;
  uint64_t version = 0;
  uint64_t length = 0;
  std::vector<uint32_t> checksums;

  ~ChunkEntry() {
    if (chunk_fd >= 0) ::close(chunk_fd);
    if (meta_fd >= 0) ::close(meta_fd);
  }
};

namespace {

constexpr size_t kMetaHeaderSize = 8;

bool preadAll(int fd, char* buf, size_t len, uint64_t offset) {
  size_t done = 0;
  while (done < len) {
    ssize_t n = ::pread(fd, buf + done, len - done, static_cast<off_t>(offset + done));
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (n == 0) return false;
    done += static_cast<size_t>(n);
  }
  return true;
}

bool pwriteAll(int fd, const char* buf, size_t len, uint64_t offset) {
  size_t done = 0;
  while (done < len) {
    ssize_t n = ::pwrite(fd, buf + done, len - done, static_cast<off_t>(offset + done));
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    done += static_cast<size_t>(n);
  }
  return true;
}

void encodeU64(char* out, uint64_t v) {
  for (int i = 0; i < 8; ++i) out[i] = static_cast<char>((v >> (8 * i)) & 0xff);
}

uint64_t decodeU64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

void encodeU32(char* out, uint32_t v) {
  for (int i = 0; i < 4; ++i) out[i] = static_cast<char>((v >> (8 * i)) & 0xff);
}

uint32_t decodeU32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

uint64_t fileSize(int fd) {
  struct stat st{};
  if (::fstat(fd, &st) != 0) return 0;
  return static_cast<uint64_t>(st.st_size);
}

bool writeVersion(int meta_fd, uint64_t version) {
  char buf[8];
  encodeU64(buf, version);
  return pwriteAll(meta_fd, buf, 8, 0) && ::fsync(meta_fd) == 0;
}

}

MutationLock::MutationLock(std::shared_ptr<ChunkEntry> entry) : entry_(std::move(entry)), lock_(entry_->mutation_mutex) {}

ChunkStore::ChunkStore(std::string data_dir, uint64_t chunk_size, uint64_t checksum_block_size)
    : data_dir_(std::move(data_dir)), chunk_size_(chunk_size), block_size_(checksum_block_size) {
  fs::create_directories(data_dir_);
}

ChunkStore::~ChunkStore() = default;

std::string ChunkStore::chunkPath(uint64_t handle) const {
  return data_dir_ + "/" + handleToHex(handle) + ".chunk";
}

std::string ChunkStore::metaPath(uint64_t handle) const {
  return data_dir_ + "/" + handleToHex(handle) + ".meta";
}

std::shared_ptr<ChunkEntry> ChunkStore::find(uint64_t handle) {
  std::lock_guard<std::mutex> lock(table_mutex_);
  auto it = entries_.find(handle);
  return it == entries_.end() ? nullptr : it->second;
}

std::shared_ptr<ChunkEntry> ChunkStore::openEntry(uint64_t handle, bool create_new, uint64_t version) {
  auto entry = std::make_shared<ChunkEntry>();
  int flags = create_new ? (O_RDWR | O_CREAT | O_EXCL) : O_RDWR;
  entry->chunk_fd = ::open(chunkPath(handle).c_str(), flags, 0644);
  if (entry->chunk_fd < 0) return nullptr;
  entry->meta_fd = ::open(metaPath(handle).c_str(), flags, 0644);
  if (entry->meta_fd < 0) {
    if (create_new) ::unlink(chunkPath(handle).c_str());
    return nullptr;
  }
  if (create_new) {
    entry->version = version;
    if (!writeVersion(entry->meta_fd, version)) return nullptr;
    return entry;
  }
  uint64_t meta_size = fileSize(entry->meta_fd);
  if (meta_size < kMetaHeaderSize) return nullptr;
  char header[8];
  if (!preadAll(entry->meta_fd, header, 8, 0)) return nullptr;
  entry->version = decodeU64(header);
  entry->length = fileSize(entry->chunk_fd);
  size_t stored = static_cast<size_t>((meta_size - kMetaHeaderSize) / 4);
  size_t needed = static_cast<size_t>((entry->length + block_size_ - 1) / block_size_);
  size_t loaded = std::min(stored, needed);
  std::string raw(loaded * 4, '\0');
  if (loaded > 0 && !preadAll(entry->meta_fd, raw.data(), raw.size(), kMetaHeaderSize)) return nullptr;
  entry->checksums.resize(loaded);
  for (size_t i = 0; i < loaded; ++i) entry->checksums[i] = decodeU32(raw.data() + 4 * i);
  return entry;
}

void ChunkStore::scan() {
  std::vector<uint64_t> chunks;
  std::vector<uint64_t> metas;
  for (const auto& item : fs::directory_iterator(data_dir_)) {
    if (!item.is_regular_file()) continue;
    std::string stem = item.path().stem().string();
    std::string ext = item.path().extension().string();
    uint64_t handle = 0;
    if (!hexToHandle(stem, &handle)) continue;
    if (ext == ".chunk") chunks.push_back(handle);
    else if (ext == ".meta") metas.push_back(handle);
  }
  std::sort(chunks.begin(), chunks.end());
  std::sort(metas.begin(), metas.end());
  for (uint64_t handle : metas) {
    if (!std::binary_search(chunks.begin(), chunks.end(), handle)) {
      GFS_LOG_WARN << "removing orphan meta for chunk " << handleToHex(handle);
      ::unlink(metaPath(handle).c_str());
    }
  }
  std::lock_guard<std::mutex> lock(table_mutex_);
  for (uint64_t handle : chunks) {
    if (!std::binary_search(metas.begin(), metas.end(), handle)) {
      GFS_LOG_WARN << "chunk " << handleToHex(handle) << " has no meta file, ignoring it";
      continue;
    }
    auto entry = openEntry(handle, false, 0);
    if (!entry) {
      GFS_LOG_WARN << "chunk " << handleToHex(handle) << " could not be opened, ignoring it";
      continue;
    }
    size_t needed = static_cast<size_t>((entry->length + block_size_ - 1) / block_size_);
    if (entry->checksums.size() < needed) {
      GFS_LOG_WARN << "chunk " << handleToHex(handle) << " is missing checksums for its tail, those blocks read as corrupt";
    }
    entries_[handle] = std::move(entry);
  }
  GFS_LOG_INFO << "scanned " << entries_.size() << " chunks in " << data_dir_;
}

rpc::ResultCode ChunkStore::create(uint64_t handle, uint64_t version) {
  std::lock_guard<std::mutex> lock(table_mutex_);
  if (entries_.count(handle)) return rpc::FAILED;
  auto entry = openEntry(handle, true, version);
  if (!entry) return rpc::FAILED;
  entries_[handle] = std::move(entry);
  return rpc::OK;
}

rpc::ResultCode ChunkStore::createCopy(uint64_t handle, uint64_t version, uint64_t copy_from) {
  auto source = find(copy_from);
  if (!source) return rpc::NO_SUCH_CHUNK;
  std::lock_guard<std::mutex> table_lock(table_mutex_);
  if (entries_.count(handle)) return rpc::FAILED;
  auto entry = openEntry(handle, true, version);
  if (!entry) return rpc::FAILED;
  std::lock_guard<std::mutex> source_lock(source->state_mutex);
  std::string buf(1 << 20, '\0');
  uint64_t done = 0;
  while (done < source->length) {
    size_t step = static_cast<size_t>(std::min<uint64_t>(buf.size(), source->length - done));
    if (!preadAll(source->chunk_fd, buf.data(), step, done) || !pwriteAll(entry->chunk_fd, buf.data(), step, done)) {
      return rpc::FAILED;
    }
    done += step;
  }
  entry->length = source->length;
  entry->checksums = source->checksums;
  std::string raw(entry->checksums.size() * 4, '\0');
  for (size_t i = 0; i < entry->checksums.size(); ++i) encodeU32(raw.data() + 4 * i, entry->checksums[i]);
  if (!raw.empty() && !pwriteAll(entry->meta_fd, raw.data(), raw.size(), kMetaHeaderSize)) return rpc::FAILED;
  if (::fsync(entry->chunk_fd) != 0 || ::fsync(entry->meta_fd) != 0) return rpc::FAILED;
  entries_[handle] = std::move(entry);
  return rpc::OK;
}

bool ChunkStore::recomputeBlocks(ChunkEntry& entry, uint64_t first_block, uint64_t last_block) {
  size_t needed = static_cast<size_t>((entry.length + block_size_ - 1) / block_size_);
  if (entry.checksums.size() < needed) entry.checksums.resize(needed, 0);
  if (needed == 0) return true;
  last_block = std::min<uint64_t>(last_block, needed - 1);
  if (first_block > last_block) return true;
  uint64_t start = first_block * block_size_;
  uint64_t end = std::min<uint64_t>((last_block + 1) * block_size_, entry.length);
  std::string buf(static_cast<size_t>(end - start), '\0');
  if (!preadAll(entry.chunk_fd, buf.data(), buf.size(), start)) return false;
  std::string raw(static_cast<size_t>(last_block - first_block + 1) * 4, '\0');
  for (uint64_t b = first_block; b <= last_block; ++b) {
    uint64_t bs = b * block_size_ - start;
    uint64_t be = std::min<uint64_t>(bs + block_size_, buf.size());
    uint32_t crc = crc32(std::string_view(buf).substr(static_cast<size_t>(bs), static_cast<size_t>(be - bs)));
    entry.checksums[static_cast<size_t>(b)] = crc;
    encodeU32(raw.data() + 4 * (b - first_block), crc);
  }
  return pwriteAll(entry.meta_fd, raw.data(), raw.size(), kMetaHeaderSize + 4 * first_block);
}

rpc::ResultCode ChunkStore::read(uint64_t handle, uint64_t offset, uint64_t length, std::string* out) {
  auto entry = find(handle);
  if (!entry) return rpc::NO_SUCH_CHUNK;
  std::lock_guard<std::mutex> lock(entry->state_mutex);
  if (offset > entry->length) return rpc::OUT_OF_RANGE;
  uint64_t end = std::min<uint64_t>(offset + length, entry->length);
  out->clear();
  if (end == offset) return rpc::OK;
  uint64_t first = offset / block_size_;
  uint64_t last = (end - 1) / block_size_;
  if (last >= entry->checksums.size()) {
    markCorrupt(handle);
    return rpc::CHECKSUM_MISMATCH;
  }
  uint64_t start = first * block_size_;
  uint64_t stop = std::min<uint64_t>((last + 1) * block_size_, entry->length);
  std::string buf(static_cast<size_t>(stop - start), '\0');
  if (!preadAll(entry->chunk_fd, buf.data(), buf.size(), start)) return rpc::FAILED;
  for (uint64_t b = first; b <= last; ++b) {
    uint64_t bs = b * block_size_ - start;
    uint64_t be = std::min<uint64_t>(bs + block_size_, buf.size());
    if (crc32(std::string_view(buf).substr(static_cast<size_t>(bs), static_cast<size_t>(be - bs))) != entry->checksums[static_cast<size_t>(b)]) {
      markCorrupt(handle);
      return rpc::CHECKSUM_MISMATCH;
    }
  }
  *out = buf.substr(static_cast<size_t>(offset - start), static_cast<size_t>(end - offset));
  return rpc::OK;
}

rpc::ResultCode ChunkStore::write(uint64_t handle, uint64_t offset, std::string_view data) {
  auto entry = find(handle);
  if (!entry) return rpc::NO_SUCH_CHUNK;
  if (offset + data.size() > chunk_size_) return rpc::OUT_OF_RANGE;
  if (data.empty()) return rpc::OK;
  std::lock_guard<std::mutex> lock(entry->state_mutex);
  if (!pwriteAll(entry->chunk_fd, data.data(), data.size(), offset)) return rpc::FAILED;
  uint64_t old_length = entry->length;
  entry->length = std::max<uint64_t>(old_length, offset + data.size());
  uint64_t first = std::min(offset, old_length) / block_size_;
  uint64_t last = (entry->length - 1) / block_size_;
  if (!recomputeBlocks(*entry, first, last)) return rpc::FAILED;
  if (::fsync(entry->chunk_fd) != 0 || ::fsync(entry->meta_fd) != 0) return rpc::FAILED;
  return rpc::OK;
}

rpc::ResultCode ChunkStore::pad(uint64_t handle, uint64_t from_offset) {
  auto entry = find(handle);
  if (!entry) return rpc::NO_SUCH_CHUNK;
  if (from_offset > chunk_size_) return rpc::OUT_OF_RANGE;
  std::lock_guard<std::mutex> lock(entry->state_mutex);
  uint64_t old_length = entry->length;
  if (from_offset >= old_length) {
    if (::ftruncate(entry->chunk_fd, static_cast<off_t>(chunk_size_)) != 0) return rpc::FAILED;
  } else {
    std::string zeros(1 << 20, '\0');
    uint64_t pos = from_offset;
    while (pos < chunk_size_) {
      size_t step = static_cast<size_t>(std::min<uint64_t>(zeros.size(), chunk_size_ - pos));
      if (!pwriteAll(entry->chunk_fd, zeros.data(), step, pos)) return rpc::FAILED;
      pos += step;
    }
  }
  entry->length = chunk_size_;
  uint64_t first = std::min(from_offset, old_length) / block_size_;
  uint64_t last = (chunk_size_ - 1) / block_size_;
  if (!recomputeBlocks(*entry, first, last)) return rpc::FAILED;
  if (::fsync(entry->chunk_fd) != 0 || ::fsync(entry->meta_fd) != 0) return rpc::FAILED;
  return rpc::OK;
}

rpc::ResultCode ChunkStore::length(uint64_t handle, uint64_t* out) {
  auto entry = find(handle);
  if (!entry) return rpc::NO_SUCH_CHUNK;
  std::lock_guard<std::mutex> lock(entry->state_mutex);
  *out = entry->length;
  return rpc::OK;
}

std::optional<uint64_t> ChunkStore::version(uint64_t handle) {
  auto entry = find(handle);
  if (!entry) return std::nullopt;
  std::lock_guard<std::mutex> lock(entry->state_mutex);
  return entry->version;
}

rpc::ResultCode ChunkStore::setVersion(uint64_t handle, uint64_t version) {
  auto entry = find(handle);
  if (!entry) return rpc::NO_SUCH_CHUNK;
  std::lock_guard<std::mutex> lock(entry->state_mutex);
  if (entry->version == version) return rpc::OK;
  if (!writeVersion(entry->meta_fd, version)) return rpc::FAILED;
  entry->version = version;
  return rpc::OK;
}

bool ChunkStore::contains(uint64_t handle) {
  return find(handle) != nullptr;
}

bool ChunkStore::remove(uint64_t handle) {
  std::shared_ptr<ChunkEntry> entry;
  {
    std::lock_guard<std::mutex> lock(table_mutex_);
    auto it = entries_.find(handle);
    if (it == entries_.end()) return false;
    entry = std::move(it->second);
    entries_.erase(it);
  }
  ::unlink(chunkPath(handle).c_str());
  ::unlink(metaPath(handle).c_str());
  std::lock_guard<std::mutex> lock(corrupt_mutex_);
  corrupt_.erase(handle);
  return true;
}

std::vector<ChunkListing> ChunkStore::list() {
  std::vector<std::pair<uint64_t, std::shared_ptr<ChunkEntry>>> snapshot;
  {
    std::lock_guard<std::mutex> lock(table_mutex_);
    snapshot.reserve(entries_.size());
    for (const auto& [handle, entry] : entries_) snapshot.emplace_back(handle, entry);
  }
  std::vector<ChunkListing> out;
  out.reserve(snapshot.size());
  for (const auto& [handle, entry] : snapshot) {
    std::lock_guard<std::mutex> lock(entry->state_mutex);
    out.push_back({handle, entry->version, entry->length});
  }
  std::sort(out.begin(), out.end(), [](const ChunkListing& a, const ChunkListing& b) { return a.handle < b.handle; });
  return out;
}

MutationLock ChunkStore::lockForMutation(uint64_t handle) {
  auto entry = find(handle);
  if (!entry) return MutationLock();
  return MutationLock(std::move(entry));
}

void ChunkStore::markCorrupt(uint64_t handle) {
  std::lock_guard<std::mutex> lock(corrupt_mutex_);
  if (corrupt_.insert(handle).second) GFS_LOG_ERROR << "checksum mismatch on chunk " << handleToHex(handle);
}

std::vector<uint64_t> ChunkStore::corruptHandles() {
  std::lock_guard<std::mutex> lock(corrupt_mutex_);
  return {corrupt_.begin(), corrupt_.end()};
}

void ChunkStore::clearCorrupt(const std::vector<uint64_t>& handles) {
  std::lock_guard<std::mutex> lock(corrupt_mutex_);
  for (uint64_t h : handles) corrupt_.erase(h);
}

std::string loadOrCreateChunkserverId(const std::string& data_dir) {
  fs::create_directories(data_dir);
  std::string path = data_dir + "/chunkserver_id";
  std::ifstream in(path);
  std::string id;
  if (in && std::getline(in, id) && !id.empty()) return id;
  id = randomHexId();
  std::ofstream out(path, std::ios::trunc);
  out << id << "\n";
  out.close();
  return id;
}

}
