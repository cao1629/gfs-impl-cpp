#include <iostream>

#include "client/gfs_client.h"

int main(int argc, char** argv) {
  gfs::Config config = gfs::Config::fromArgs(argc > 1 ? 1 : argc, argv);
  gfs::Client client(config);
  std::cout << "not implemented" << std::endl;
  return 1;
}
