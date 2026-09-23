#include <iostream>

#include "chunkserver/chunk_store.h"
#include "chunkserver/chunkserver.h"
#include "chunkserver/chunkserver_service.h"
#include "common/config.h"
#include "common/logging.h"

int main(int argc, char** argv) {
  gfs::SetLogTag("chunkserver");
  gfs::Config config = gfs::Config::FromArgs(argc, argv);
  if (config.data_dir.empty()) {
    std::cerr << "--data_dir is required\n" << gfs::Config::Usage();
    return 2;
  }
  gfs::ChunkStore store(config.data_dir, config.chunk_size,
                        config.checksum_block_size);
  store.Scan();
  std::string id = gfs::LoadOrCreateChunkserverId(config.data_dir);
  gfs::Chunkserver chunkserver(config, id, &store);
  gfs::ChunkserverService service(&chunkserver);
  int port = 0;
  auto server = gfs::StartChunkserverServer(&service, config.listen,
                                            config.chunk_size, &port);
  if (!server) {
    GFS_LOG_ERROR << "could not listen on " << config.listen;
    return 1;
  }
  chunkserver.SetAdvertiseAddress(config.EffectiveAdvertise());
  chunkserver.StartHeartbeat();
  GFS_LOG_INFO << "chunkserver " << id << " listening on " << config.listen
               << ", data in " << config.data_dir << ", master "
               << config.master_address;
  server->Wait();
  chunkserver.Stop();
  return 0;
}
