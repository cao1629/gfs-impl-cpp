#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <sys/types.h>

#include "client/gfs_client.h"
#include "gfs.grpc.pb.h"

namespace gfs::testing {

class LocalCluster {
 public:
  explicit LocalCluster(int chunkservers, std::map<std::string, std::string> overrides = {});
  ~LocalCluster();

  LocalCluster(const LocalCluster&) = delete;
  LocalCluster& operator=(const LocalCluster&) = delete;

  Config clientConfig() const;
  std::unique_ptr<Client> client() const;
  std::unique_ptr<rpc::Master::Stub> masterStub() const;

  const std::string& masterAddress() const { return master_.address; }
  const std::string& chunkserverAddress(int i) const { return chunkservers_[i].address; }
  const std::string& chunkserverDataDir(int i) const { return chunkservers_[i].data_dir; }
  const std::string& masterDataDir() const { return master_.data_dir; }
  int chunkserverCount() const { return static_cast<int>(chunkservers_.size()); }

  void killChunkserver(int i);
  void restartChunkserver(int i);
  void killMaster();
  void restartMaster();

  size_t chunkFilesOnDisk() const;
  std::string flag(const std::string& key) const;

 private:
  struct Process {
    pid_t pid = -1;
    std::string address;
    std::string data_dir;
    std::string rack;
    std::string log_path;
  };

  void spawnMaster();
  void spawnChunkserver(int i);
  void kill(Process* p);
  std::vector<std::string> commonArgs() const;

  std::string root_;
  std::map<std::string, std::string> flags_;
  Process master_;
  std::vector<Process> chunkservers_;
};

std::string freePort();
void sleepMs(int64_t ms);

}
