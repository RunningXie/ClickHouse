cmake_minimum_required(VERSION 3.20)

message(STATUS "start handle contrib/cubefs-cmake/CMakeLists.txt")
set(CUBEFS_DIR "${ClickHouse_SOURCE_DIR}/contrib/cubefs")


set(TARGET libsdk)
set(SRCS "${CUBEFS_DIR}/libsdk/libsdk.go")
set(LIB libsdk.so)


add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${LIB}
  DEPENDS ${SRCS}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND env GOPATH=${GOPATH} go build -buildmode=c-archive
  -o "${CMAKE_CURRENT_BINARY_DIR}/${LIB}"
  ${CMAKE_GO_FLAGS} $SRC
  COMMENT "Building Go library")

add_custom_target(${TARGET} DEPENDS ${LIB} ${HEADER})
add_library(ch_contrib::cubefs STATIC IMPORTED GLOBAL)
add_dependencies(ch_contrib::cubefs ${TARGET})
set_target_properties(ch_contrib::cubefs
  PROPERTIES
  IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/${LIB}
  INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_BINARY_DIR})






#message(STATUS "start execute_process")
#execute_process(
#    COMMAND make libsdk
#    WORKING_DIRECTORY ${CUBEFS_DIR})
##创建库：_cubefs, STATIC: 表示静态库， IMPORTED: 表示不是通过cmake生成，而是通过外部直接导入，后面通常需要配合set_target_properties来指定库路径
#add_library(_cubefs STATIC IMPORTED GLOBAL)
##常见用法：先通过set_target_properties设置导入属性，再通过target_link_libraries链接依赖库
#set_target_properties(_cubefs PROPERTIES
#    IMPORTED_LOCATION "${CUBEFS_DIR}/build/bin/libcfs.a"
#)
#add_library(ch_contrib::cubefs ALIAS _cubefs)
# 定义模板文件路径（也是生成文件路径）
set(FILE_PATH "${CUBEFS_DIR}/libsdk/libcfs.h")
# 检查文件是否包含指定内容
#file(STRINGS ${FILE_PATH} FILE_CONTENTS)
#list(FIND FILE_CONTENTS "#pragma GCC diagnostic push" PRAGMA_INDEX)
#if (PRAGMA_INDEX EQUAL -1)
#  # 在头部插入指令
#  execute_process(
#    COMMAND sh -c "echo '#pragma GCC diagnostic push\n#pragma GCC diagnostic ignored \"-Wreserved-identifier\"\n#pragma GCC diagnostic ignored \"-Wmacro-redefined\"\n' | cat - ${FILE_PATH}"
#    OUTPUT_FILE "${FILE_PATH}.tmp"
#  )
#  # 在尾部插入指令
#  execute_process(
#    COMMAND sh -c "echo '#pragma GCC diagnostic pop' >> ${FILE_PATH}.tmp"
#  )
#  # 重命名生成文件（覆盖原始文件）
#  execute_process(
#    COMMAND mv "${FILE_PATH}.tmp" "${FILE_PATH}"
#  )
#endif()


