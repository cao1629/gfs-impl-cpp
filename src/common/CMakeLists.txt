file(GLOB GFS_COMMON_SRCS CONFIGURE_DEPENDS *.cc)
add_library(gfs_common STATIC ${GFS_COMMON_SRCS})
target_include_directories(gfs_common PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(gfs_common PUBLIC gfs_proto ZLIB::ZLIB Threads::Threads)
