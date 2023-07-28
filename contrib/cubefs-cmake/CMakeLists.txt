option(ENABLE_CUBEFS "Enable cubefs" OFF)

if (NOT ENABLE_CUBEFS)
    message(STATUS "Not using cubefs")
    return()
endif()

set(CUBEFS_DIR "${ClickHouse_SOURCE_DIR}/contrib/cubefs")

set(SRCS
    "${CUBEFS_DIR}/src/cube.cc"
    "${CUBEFS_DIR}/src/file_cache.cc"
    "${CUBEFS_DIR}/src/file_view.cc"
    "${CUBEFS_DIR}/src/file.cc"
    "${CUBEFS_DIR}/src/hash.cc"
    "${CUBEFS_DIR}/src/inode.cc"
    "${CUBEFS_DIR}/src/io.cc"
    "${CUBEFS_DIR}/src/logging.cc"
    "${CUBEFS_DIR}/src/metrics.cc"
    "${CUBEFS_DIR}/src/mount.cc"
    "${CUBEFS_DIR}/src/options.cc"
    "${CUBEFS_DIR}/src/rate_limiter.cc"
    "${CUBEFS_DIR}/src/statsd.cc"
    "${CUBEFS_DIR}/src/util.cc"
)

add_library(_cubefs ${SRCS})

target_include_directories(_cubefs PUBLIC "${CUBEFS_DIR}/include")

target_link_libraries(_cubefs PUBLIC pthread)

add_library(ch_contrib::cubefs ALIAS _cubefs)