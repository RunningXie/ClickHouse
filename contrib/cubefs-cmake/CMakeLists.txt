message(STATUS "start handle contrib/cubefs-cmake/CMakeLists.txt")
set(CUBEFS_DIR "${ClickHouse_SOURCE_DIR}/contrib/cubefs")
message(STATUS "start execute_process")
execute_process(
    COMMAND make libsdk
    WORKING_DIRECTORY ${CUBEFS_DIR})
message(STATUS "start add_library")
add_library(_cubefs STATIC IMPORTED GLOBAL)
message(STATUS "start target_include_directories")
set_target_properties(_cubefs PROPERTIES
    IMPORTED_LOCATION "${CUBEFS_DIR}/build/bin/libcfs.a"
    INTERFACE_INCLUDE_DIRECTORIES "${CUBEFS_DIR}/libsdk/libcfs.h"
)
message(STATUS "start add_library")
add_library(ch_contrib::cubefs ALIAS _cubefs)
message(STATUS "end handle contrib/cubefs-cmake/CMakeLists.txt")
