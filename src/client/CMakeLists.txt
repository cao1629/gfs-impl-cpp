file(GLOB GFS_CLIENT_SRCS CONFIGURE_DEPENDS *.cc)
add_library(gfs_client STATIC ${GFS_CLIENT_SRCS})
target_link_libraries(gfs_client PUBLIC gfs_common)
