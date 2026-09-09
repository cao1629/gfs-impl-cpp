cmake_minimum_required(VERSION 3.25)
project(gfs_impl_cpp CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)

add_library(gfs_proto proto/gfs.proto proto/master_state.proto)
target_link_libraries(gfs_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)
target_include_directories(gfs_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/gen)
protobuf_generate(TARGET gfs_proto LANGUAGE cpp
  PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/gen IMPORT_DIRS proto)
protobuf_generate(TARGET gfs_proto LANGUAGE grpc
  GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
  PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
  PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/gen IMPORT_DIRS proto)

add_subdirectory(src/common)
add_subdirectory(src/master)
add_subdirectory(src/chunkserver)
add_subdirectory(src/client)
add_subdirectory(src/cli)

option(GFS_BUILD_TESTS "Build the test binaries" ON)
if(GFS_BUILD_TESTS)
  find_package(GTest CONFIG REQUIRED)
  enable_testing()
  add_subdirectory(tests)
endif()
