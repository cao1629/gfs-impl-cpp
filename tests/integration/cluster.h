#pragma once

#include <sys/types.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "client/gfs_client.h"
#include "gfs.grpc.pb.h"

namespace gfs::testing {

class LocalCluster {
 public:
  explicit LocalCluster(int chunkservers,
                        std::map<std::string, std::string> overrides = {});
  ~LocalCluster();

  LocalCluster(const LocalCluster&) = delete;
  LocalCluster& operator=(const LocalCluster&) = delete;

  Config ClientConfig() const;
  // NOLINTNEXTLINE(readability-identifier-naming)
  std::unique_ptr<Client> client() const;
  std::unique_ptr<rpc::Master::Stub> MasterStub() const;

  const std::string& MasterAddress() const { return master_.address; }
  const std::string& ChunkserverAddress(int i) const {
    return chunkservers_[i].address;
  }
  const std::string& ChunkserverDataDir(int i) const {
    return chunkservers_[i].data_dir;
  }
  const std::string& MasterDataDir() const { return master_.data_dir; }
  int ChunkserverCount() const {
    return static_cast<int>(chunkservers_.size());
  }

  void KillChunkserver(int i);
  void RestartChunkserver(int i);
  void KillMaster();
  void RestartMaster();

  size_t ChunkFilesOnDisk() const;
  std::string Flag(const std::string& key) const;

 private:
  struct Process {
    pid_t pid = -1;
    std::string address;
    std::string data_dir;
    std::string rack;
    std::string log_path;
  };

  void SpawnMaster();
  void SpawnChunkserver(int i);
  void Kill(Process* p);
  std::vector<std::string> CommonArgs() const;

  std::string root_;
  std::map<std::string, std::string> flags_;
  Process master_;
  std::vector<Process> chunkservers_;
};

std::string FreePort();
void SleepMs(int64_t ms);

}  // namespace gfs::testing
