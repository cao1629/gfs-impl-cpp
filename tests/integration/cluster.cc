#include "cluster.h"

#include <arpa/inet.h>
#include <fcntl.h>
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

#include <grpcpp/grpcpp.h>

extern char** environ;

namespace gfs::testing {

namespace fs = std::filesystem;

std::string freePort() {
  int sock = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) throw std::runtime_error("socket failed");
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  int one = 1;
  ::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  if (::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) throw std::runtime_error("bind failed");
  socklen_t len = sizeof(addr);
  ::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len);
  int port = ntohs(addr.sin_port);
  ::close(sock);
  return std::to_string(port);
}

void sleepMs(int64_t ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

namespace {

std::map<std::string, std::string> defaultFlags() {
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

pid_t spawn(const std::string& binary, const std::vector<std::string>& args, const std::string& log_path) {
  std::vector<std::string> argv_storage;
  argv_storage.push_back(binary);
  for (const auto& a : args) argv_storage.push_back(a);
  std::vector<char*> argv;
  for (auto& s : argv_storage) argv.push_back(s.data());
  argv.push_back(nullptr);
  posix_spawn_file_actions_t actions;
  posix_spawn_file_actions_init(&actions);
  posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  pid_t pid = -1;
  int rc = posix_spawn(&pid, binary.c_str(), &actions, nullptr, argv.data(), environ);
  posix_spawn_file_actions_destroy(&actions);
  if (rc != 0) throw std::runtime_error("posix_spawn failed for " + binary);
  return pid;
}

template <typename Probe>
void waitUntil(Probe probe, const std::string& what, int64_t timeout_ms = 15000) {
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (probe()) return;
    sleepMs(50);
  }
  throw std::runtime_error("timed out waiting for " + what);
}

void waitForMaster(const std::string& address) {
  auto stub = rpc::Master::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
  waitUntil([&] {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(300));
    rpc::GetClusterInfoRequest req;
    rpc::GetClusterInfoResponse resp;
    return stub->GetClusterInfo(&ctx, req, &resp).ok();
  }, "master at " + address);
}

void waitForChunkserver(const std::string& address) {
  auto stub = rpc::Chunkserver::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
  waitUntil([&] {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(300));
    rpc::GetChunkLengthRequest req;
    req.set_handle(0);
    rpc::GetChunkLengthResponse resp;
    return stub->GetChunkLength(&ctx, req, &resp).ok();
  }, "chunkserver at " + address);
}

}

LocalCluster::LocalCluster(int chunkservers, std::map<std::string, std::string> overrides) {
  flags_ = defaultFlags();
  for (auto& [k, v] : overrides) flags_[k] = v;
  root_ = (fs::temp_directory_path() / ("gfs-test-" + std::to_string(::getpid()) + "-" + freePort())).string();
  fs::create_directories(root_);
  master_.address = "127.0.0.1:" + freePort();
  master_.data_dir = root_ + "/master";
  master_.log_path = root_ + "/master.log";
  fs::create_directories(master_.data_dir);
  spawnMaster();
  waitForMaster(master_.address);
  for (int i = 0; i < chunkservers; ++i) {
    Process p;
    p.address = "127.0.0.1:" + freePort();
    p.data_dir = root_ + "/cs" + std::to_string(i);
    p.rack = "rack" + std::to_string(i % 2);
    p.log_path = root_ + "/cs" + std::to_string(i) + ".log";
    fs::create_directories(p.data_dir);
    chunkservers_.push_back(p);
    spawnChunkserver(i);
  }
  for (int i = 0; i < chunkservers; ++i) waitForChunkserver(chunkservers_[i].address);
  Millis interval;
  parseDuration(flags_["heartbeat_interval"], &interval);
  sleepMs(interval.count() * 3);
}

LocalCluster::~LocalCluster() {
  for (auto& p : chunkservers_) kill(&p);
  kill(&master_);
  if (::getenv("GFS_KEEP_TEST_DIRS") == nullptr) {
    std::error_code ec;
    fs::remove_all(root_, ec);
  } else {
    fprintf(stderr, "test cluster kept at %s\n", root_.c_str());
  }
}

std::vector<std::string> LocalCluster::commonArgs() const {
  std::vector<std::string> args;
  for (const auto& [k, v] : flags_) args.push_back("--" + k + "=" + v);
  return args;
}

void LocalCluster::spawnMaster() {
  auto args = commonArgs();
  args.push_back("--listen=" + master_.address);
  args.push_back("--data_dir=" + master_.data_dir);
  master_.pid = spawn(std::string(GFS_BIN_DIR) + "/gfs_master", args, master_.log_path);
}

void LocalCluster::spawnChunkserver(int i) {
  Process& p = chunkservers_[i];
  auto args = commonArgs();
  args.push_back("--listen=" + p.address);
  args.push_back("--master_address=" + master_.address);
  args.push_back("--data_dir=" + p.data_dir);
  args.push_back("--rack=" + p.rack);
  p.pid = spawn(std::string(GFS_BIN_DIR) + "/gfs_chunkserver", args, p.log_path);
}

void LocalCluster::kill(Process* p) {
  if (p->pid <= 0) return;
  ::kill(p->pid, SIGKILL);
  int status = 0;
  ::waitpid(p->pid, &status, 0);
  p->pid = -1;
}

void LocalCluster::killChunkserver(int i) { kill(&chunkservers_[i]); }

void LocalCluster::restartChunkserver(int i) {
  kill(&chunkservers_[i]);
  spawnChunkserver(i);
  waitForChunkserver(chunkservers_[i].address);
}

void LocalCluster::killMaster() { kill(&master_); }

void LocalCluster::restartMaster() {
  kill(&master_);
  spawnMaster();
  waitForMaster(master_.address);
  Millis interval;
  parseDuration(flags_.at("heartbeat_interval"), &interval);
  sleepMs(interval.count() * 3);
}

Config LocalCluster::clientConfig() const {
  Config config;
  for (const auto& [k, v] : flags_) Config::set(config, k, v);
  config.master_address = master_.address;
  return config;
}

std::unique_ptr<Client> LocalCluster::client() const {
  return std::make_unique<Client>(clientConfig());
}

std::unique_ptr<rpc::Master::Stub> LocalCluster::masterStub() const {
  return rpc::Master::NewStub(grpc::CreateChannel(master_.address, grpc::InsecureChannelCredentials()));
}

size_t LocalCluster::chunkFilesOnDisk() const {
  size_t count = 0;
  for (const auto& p : chunkservers_) {
    for (const auto& entry : fs::directory_iterator(p.data_dir)) {
      if (entry.path().extension() == ".chunk") ++count;
    }
  }
  return count;
}

std::string LocalCluster::flag(const std::string& key) const {
  return flags_.at(key);
}

}
