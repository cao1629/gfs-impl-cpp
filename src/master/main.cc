#include <signal.h>

#include <iostream>

#include <grpcpp/grpcpp.h>

#include "common/config.h"
#include "common/logging.h"
#include "master/master.h"
#include "master/master_service.h"

int main(int argc, char** argv) {
  gfs::setLogTag("master");
  gfs::Config config = gfs::Config::fromArgs(argc, argv);
  if (config.data_dir.empty()) {
    std::cerr << "--data_dir is required\n";
    return 2;
  }
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);

  gfs::Master master(config);
  master.start();
  gfs::MasterService service(master);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(config.listen, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  if (!server) {
    GFS_LOG_ERROR << "cannot listen on " << config.listen;
    return 1;
  }
  GFS_LOG_INFO << "listening on " << config.listen << ", data in " << config.data_dir;
  int signal = 0;
  sigwait(&signals, &signal);
  GFS_LOG_INFO << "shutting down on signal " << signal;
  server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
  master.stop();
  return 0;
}
