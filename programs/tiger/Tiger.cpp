#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libcfs.h"

namespace DB
{
    namespace ErrorCodes
        {

        }
}

int mainEntryClickHouseTiger(int argc, char ** argv)
{
    int64_t id = cfs_new_client();
    if (id <= 0)
    {
        printf("Failed to create client\n");
        return -1;
    }

    // 设置客户端信息
    if (cfs_set_client(id, "volName", "xieyichen") != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        return -1;
    }
    if (cfs_set_client(id, "masterAddr", "cfs-south.oppo.local") != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        return -1;
    }
    if (cfs_set_client(id, "logDir", "/home/service/var/logs/cfs/test-log") != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        return -1;
    }
    if (cfs_set_client(id, "logLevel", "debug") != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        return -1;
    }
    if (cfs_set_client(id, "accessKey", "jRlZO65q7XlH5bnV") != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        return -1;
    }
    if (cfs_set_client(id, "secretKey", "V1m730UzREHaK1jCkC0kL0cewOX0kH3K") != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        return -1;
    }
    if (cfs_set_client(id, "pushAddr", "cfs.dg-push.wanyol.com") != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        return -1;
    }

    // 启动客户端
    if (cfs_start_client(id) != 0)
    {
        printf("Failed to start client \n");
        cfs_close_client(id);
        return -1;
    }

    // 打开文件并读写内容
    int fd = cfs_open(id, "/test_dir/file.txt", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0)
    {
        printf("Failed to open file\n");
        cfs_close_client(id);
        return -1;
    }

    const char * data = "Hello, world!";
    if (cfs_write(id, fd, (void *)data, strlen(data), 0) < 0)
    {
        printf("Failed to write file\n");
        cfs_close(id, fd);
        cfs_close_client(id);
        return -1;
    }

    char buffer[1024];
    memset(buffer, 0, sizeof(buffer));
    if (cfs_read(id, fd, (void *)buffer, sizeof(buffer), 0) < 0)
    {
        printf("Failed to read file\n");
        cfs_close(id, fd);
        cfs_close_client(id);
        return -1;
    }

    printf("Read file content: %s\n", buffer);

    // 关闭文件和客户端
    cfs_close(id, fd);
    cfs_close_client(id);
    return 0;
}
