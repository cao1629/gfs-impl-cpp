#include <iostream>

#include "common/config.h"

int main(int argc, char** argv) {
  gfs::Config config = gfs::Config::fromArgs(argc, argv);
  std::cout << "not implemented, listen=" << config.listen << std::endl;
  return 1;
}
