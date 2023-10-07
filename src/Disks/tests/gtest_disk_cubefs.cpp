#include <gtest/gtest.h>
#define RUN_CUBEFS_TEST 1
#if RUN_CUBEFS_TEST

#    include <Disks/CubeFS/DiskCubeFS.h>

namespace DB
{
int64_t getClientId();
void setClientInfo(int id, const char * key, char * value);
}

const String path = "/test_dir";
const String vol_name = "xieyichen";
const String master_addr = "cfs-south.oppo.local";
const String log_dir = "/home/xieyichen/logs/cfs2/test-log";
const String log_level = "debug";
const String access_key = "jRlZO65q7XlH5bnV";
const String secret_key = "V1m730UzREHaK1jCkC0kL0cewOX0kH3K";
const String push_addr = "cfs.dg-push.wanyol.com";
const String file_name = "test.txt";

TEST(DiskTestCubeFS, RemoveFileCubeFS)
{
    auto id = DB::getClientId();
    std::cout << "get id: " << id << std::endl;
    auto settings
        = std::make_unique<DB::DiskCubeFSSettings>(id, vol_name, master_addr, log_dir, log_level, access_key, secret_key, push_addr);


    if (cfs_set_client(id, strdup("volName"), strdup("xieyichen")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //    return -1;
    }
    if (cfs_set_client(id, strdup("masterAddr"), strdup("cfs-south.oppo.local")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //   return -1;
    }
    if (cfs_set_client(id, strdup("logDir"), strdup("/home/xieyichen/logs/cfs4/test-log")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //   return -1;
    }
    if (cfs_set_client(id, strdup("logLevel"), strdup("debug")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //   return -1;
    }
    if (cfs_set_client(id, strdup("accessKey"), strdup("jRlZO65q7XlH5bnV")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //   return -1;
    }
    if (cfs_set_client(id, strdup("secretKey"), strdup("V1m730UzREHaK1jCkC0kL0cewOX0kH3K")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //   return -1;
    }
    if (cfs_set_client(id, strdup("pushAddr"), strdup("cfs.dg-push.wanyol.com")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //   return -1;
    }
    /*
        DB::setClientInfo(settings->id, "volName", const_cast<char *>(settings->vol_name.data()));
        DB::setClientInfo(settings->id, "masterAddr", const_cast<char *>(settings->master_addr.data()));
        DB::setClientInfo(settings->id, "logDir", const_cast<char *>(settings->log_dir.data()));
std::cout<<"log dir: "<<const_cast<char *>(settings->log_dir.data())<<std::endl;
        DB::setClientInfo(settings->id, "logLevel", const_cast<char *>(settings->log_level.data()));
        DB::setClientInfo(settings->id, "accessKey", const_cast<char *>(settings->access_key.data()));
        DB::setClientInfo(settings->id, "secretKey", const_cast<char *>(settings->secret_key.data()));
        DB::setClientInfo(settings->id, "pushAddr", const_cast<char *>(settings->push_addr.data()));
*/
    std::cout << "set client info  successfully " << std::endl;
    std::shared_ptr<DB::IDisk> cubeFSdisk = std::make_shared<DB::DiskCubeFS>("cubefs", path, nullptr, std::move(settings));
    std::cout << "initial cubefs disk successfully " << std::endl;
    cubeFSdisk->createDirectories("test_dir1999");
    EXPECT_TRUE(cubeFSdisk->isDirectory("test_dir1999/"));
}

TEST(DiskTestCubeFS, CreateDirectories)
{
    int64_t id = cfs_new_client();
    if (id <= 0)
    {
        printf("Failed to create client\n");
        //return -1;
    }

    // 设置客户端信息
    if (cfs_set_client(id, strdup("volName"), strdup("xieyichen")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //return -1;
    }
    if (cfs_set_client(id, strdup("masterAddr"), strdup("cfs-south.oppo.local")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //return -1;
    }
    if (cfs_set_client(id, strdup("logDir"), strdup("/home/xieyichen/logs/cfs2/test-log")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //return -1;
    }
    if (cfs_set_client(id, strdup("logLevel"), strdup("debug")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //return -1;
    }
    if (cfs_set_client(id, strdup("accessKey"), strdup("jRlZO65q7XlH5bnV")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //return -1;
    }
    if (cfs_set_client(id, strdup("secretKey"), strdup("V1m730UzREHaK1jCkC0kL0cewOX0kH3K")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //return -1;
    }
    if (cfs_set_client(id, strdup("pushAddr"), strdup("cfs.dg-push.wanyol.com")) != 0)
    {
        printf("Failed to set client info\n");
        cfs_close_client(id);
        //return -1;
    }

    // 启动客户端
    if (cfs_start_client(id) != 0)
    {
        printf("Failed to start client \n");
        cfs_close_client(id);
        //return -1;
    }
}

#endif
