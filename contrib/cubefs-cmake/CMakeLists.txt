message(STATUS "start handle contrib/cubefs-cmake/CMakeLists.txt")
set(CUBEFS_DIR "${ClickHouse_SOURCE_DIR}/contrib/cubefs")
message(STATUS "start execute_process")
execute_process(
    COMMAND make libsdk
    WORKING_DIRECTORY ${CUBEFS_DIR})
#创建库：_cubefs, STATIC: 表示静态库， IMPORTED: 表示不是通过cmake生成，而是通过外部直接导入，后面通常需要配合set_target_properties来指定库路径
add_library(_cubefs STATIC IMPORTED GLOBAL)
#常见用法：先通过set_target_properties设置导入属性，再通过target_link_libraries链接依赖库
set_target_properties(_cubefs PROPERTIES
    IMPORTED_LOCATION "${CUBEFS_DIR}/build/bin/libcfs.a"
)
add_library(ch_contrib::cubefs ALIAS _cubefs)
