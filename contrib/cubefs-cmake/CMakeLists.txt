set(CUBEFS_DIR "${ClickHouse_SOURCE_DIR}/contrib/cubefs")

execute_process(
    COMMAND make libsdk
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_library(_cubefs STATIC IMPORTED GLOBAL)

target_include_directories(_cubefs PUBLIC "${CUBEFS_DIR}/libsdk/libcfs.h")

add_library(ch_contrib::cubefs ALIAS _cubefs)