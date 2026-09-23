#include "cluster.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <grpcpp/grpcpp.h>
#include <netinet/in.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <thread>

extern char** environ;

namespace gfs::testing {

namespace fs = std::filesystem;

std::string FreePort() {
  int sock = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) throw std::runtime_error("socket failed");
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  int one = 1;
  ::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  if (::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
    throw std::runtime_error("bind failed");
  socklen_t len = sizeof(addr);
  ::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len);
  int port = ntohs(addr.sin_port);
  ::close(sock);
  return std::to_string(port);
}

void SleepMs(int64_t ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

namespace {

std::map<std::string, std::string> DefaultFlags() {
  return {
      {"chunk_size", "1M"},
      {"lease_duration", "3s"},
      {"lease_clock_skew_margin", "200ms"},
      {"heartbeat_interval", "200ms"},
      {"chunkserver_dead_timeout", "1s"},
      {"deleted_file_retention", "2s"},
      {"gc_interval", "500ms"},
      {"rpc_deadline", "1s"},
      {"client_rpc_deadline", "5s"},
      {"retry_backoff_base", "50ms"},
      {"log_flush_max_delay", "5ms"},
      {"client_location_cache_ttl", "2s"},
  };
}

pid_t Spawn(const std::string& binary, const std::vector<std::string>& args,
            const std::string& log_path) {
  std::vector<std::string> argv_storage;
  argv_storage.push_back(binary);
  for (const auto& a : args) argv_storage.push_back(a);
  std::vector<char*> argv;
  for (auto& s : argv_storage) argv.push_back(s.data());
  argv.push_back(nullptr);
  posix_spawn_file_actions_t actions;
  posix_spawn_file_actions_init(&actions);
  posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, log_path.c_str(),
                                   O_WRONLY | O_CREAT | O_APPEND, 0644);
  posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, log_path.c_str(),
                                   O_WRONLY | O_CREAT | O_APPEND, 0644);
  pid_t pid = -1;
  int rc = posix_spawn(&pid, binary.c_str(), &actions, nullptr, argv.data(),
                       environ);
  posix_spawn_file_actions_destroy(&actions);
  if (rc != 0) throw std::runtime_error("posix_spawn failed for " + binary);
  return pid;
}

template <typename Probe>
void WaitUntil(Probe probe, const std::string& what,
               int64_t timeout_ms = 15000) {
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (probe()) return;
    SleepMs(50);
  }
  throw std::runtime_error("timed out waiting for " + what);
}

void WaitForMaster(const std::string& address) {
  auto stub = rpc::Master::NewStub(
      grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
  WaitUntil(
      [&] {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(300));
        rpc::GetClusterInfoRequest req;
        rpc::GetClusterInfoResponse resp;
        return stub->GetClusterInfo(&ctx, req, &resp).ok();
      },
      "master at " + address);
}

void WaitForChunkserver(const std::string& address) {
  auto stub = rpc::Chunkserver::NewStub(
      grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
  WaitUntil(
      [&] {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(300));
        rpc::GetChunkLengthRequest req;
        req.set_handle(0);
        rpc::GetChunkLengthResponse resp;
        return stub->GetChunkLength(&ctx, req, &resp).ok();
      },
      "chunkserver at " + address);
}

}  // namespace

LocalCluster::LocalCluster(int chunkservers,
                           std::map<std::string, std::string> overrides) {
  flags_ = DefaultFlags();
  for (auto& [k, v] : overrides) flags_[k] = v;
  root_ = (fs::temp_directory_path() /
           ("gfs-test-" + std::to_string(::getpid()) + "-" + FreePort()))
              .string();
  fs::create_directories(root_);
  master_.address = "127.0.0.1:" + FreePort();
  master_.data_dir = root_ + "/master";
  master_.log_path = root_ + "/master.log";
  fs::create_directories(master_.data_dir);
  SpawnMaster();
  WaitForMaster(master_.address);
  for (int i = 0; i < chunkservers; ++i) {
    Process p;
    p.address = "127.0.0.1:" + FreePort();
    p.data_dir = root_ + "/cs" + std::to_string(i);
    p.rack = "rack" + std::to_string(i % 2);
    p.log_path = root_ + "/cs" + std::to_string(i) + ".log";
    fs::create_directories(p.data_dir);
    chunkservers_.push_back(p);
    SpawnChunkserver(i);
  }
  for (int i = 0; i < chunkservers; ++i)
    WaitForChunkserver(chunkservers_[i].address);
  Millis interval;
  ParseDuration(flags_["heartbeat_interval"], &interval);
  SleepMs(interval.count() * 3);
}

LocalCluster::~LocalCluster() {
  for (auto& p : chunkservers_) Kill(&p);
  Kill(&master_);
  if (::getenv("GFS_KEEP_TEST_DIRS") == nullptr) {
    std::error_code ec;
    fs::remove_all(root_, ec);
  } else {
    fprintf(stderr, "test cluster kept at %s\n", root_.c_str());
  }
}

std::vector<std::string> LocalCluster::CommonArgs() const {
  std::vector<std::string> args;
  for (const auto& [k, v] : flags_) args.push_back("--" + k + "=" + v);
  return args;
}

void LocalCluster::SpawnMaster() {
  auto args = CommonArgs();
  args.push_back("--listen=" + master_.address);
  args.push_back("--data_dir=" + master_.data_dir);
  master_.pid =
      Spawn(std::string(GFS_BIN_DIR) + "/gfs_master", args, master_.log_path);
}

void LocalCluster::SpawnChunkserver(int i) {
  Process& p = chunkservers_[i];
  auto args = CommonArgs();
  args.push_back("--listen=" + p.address);
  args.push_back("--master_address=" + master_.address);
  args.push_back("--data_dir=" + p.data_dir);
  args.push_back("--rack=" + p.rack);
  p.pid =
      Spawn(std::string(GFS_BIN_DIR) + "/gfs_chunkserver", args, p.log_path);
}

void LocalCluster::Kill(Process* p) {
  if (p->pid <= 0) return;
  ::kill(p->pid, SIGKILL);
  int status = 0;
  ::waitpid(p->pid, &status, 0);
  p->pid = -1;
}

void LocalCluster::KillChunkserver(int i) { Kill(&chunkservers_[i]); }

void LocalCluster::RestartChunkserver(int i) {
  Kill(&chunkservers_[i]);
  SpawnChunkserver(i);
  WaitForChunkserver(chunkservers_[i].address);
}

void LocalCluster::KillMaster() { Kill(&master_); }

void LocalCluster::RestartMaster() {
  Kill(&master_);
  SpawnMaster();
  WaitForMaster(master_.address);
  Millis interval;
  ParseDuration(flags_.at("heartbeat_interval"), &interval);
  SleepMs(interval.count() * 3);
}

Config LocalCluster::ClientConfig() const {
  Config config;
  for (const auto& [k, v] : flags_) Config::Set(config, k, v);
  config.master_address = master_.address;
  return config;
}

std::unique_ptr<Client> LocalCluster::client() const {
  return std::make_unique<Client>(ClientConfig());
}

std::unique_ptr<rpc::Master::Stub> LocalCluster::MasterStub() const {
  return rpc::Master::NewStub(
      grpc::CreateChannel(master_.address, grpc::InsecureChannelCredentials()));
}

size_t LocalCluster::ChunkFilesOnDisk() const {
  size_t count = 0;
  for (const auto& p : chunkservers_) {
    for (const auto& entry : fs::directory_iterator(p.data_dir)) {
      if (entry.path().extension() == ".chunk") ++count;
    }
  }
  return count;
}

std::string LocalCluster::Flag(const std::string& key) const {
  return flags_.at(key);
}

}  // namespace gfs::testing
