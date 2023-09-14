set (CLICKHOUSE_TIGER_SOURCES Tiger.cpp clickhouse-tiger.cpp)
#在外面一层会执行target_link_libraries来链接库cubefs
set (CLICKHOUSE_TIGER_LINK dbms ch_contrib::cubefs)
#在外面一层会执行target_include_directories来链接库，这样Tiger.cpp就可以使用相对路径来引入libcfs.h
set (CLICKHOUSE_TIGER_INCLUDE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/cubefs/libsdk)
clickhouse_program_add(tiger)
