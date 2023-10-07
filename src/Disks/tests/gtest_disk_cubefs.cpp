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
const String disk_name = "cubefs";


TEST(DiskTestCubeFS, CreateDirectories)
{
    auto id = DB::getClientId();
    std::cout << "get id: " << id << std::endl;
    auto settings
        = std::make_unique<DB::DiskCubeFSSettings>(id, vol_name, master_addr, log_dir, log_level, access_key, secret_key, push_addr);

    DB::setClientInfo(settings->id, "volName", const_cast<char *>(settings->vol_name.data()));
    DB::setClientInfo(settings->id, "masterAddr", const_cast<char *>(settings->master_addr.data()));
    DB::setClientInfo(settings->id, "logDir", const_cast<char *>(settings->log_dir.data()));
    std::cout << "log dir: " << const_cast<char *>(settings->log_dir.data()) << std::endl;
    DB::setClientInfo(settings->id, "logLevel", const_cast<char *>(settings->log_level.data()));
    DB::setClientInfo(settings->id, "accessKey", const_cast<char *>(settings->access_key.data()));
    DB::setClientInfo(settings->id, "secretKey", const_cast<char *>(settings->secret_key.data()));
    DB::setClientInfo(settings->id, "pushAddr", const_cast<char *>(settings->push_addr.data()));

    std::cout << "set client info  successfully " << std::endl;
    std::shared_ptr<DB::IDisk> cubeFSdisk = std::make_shared<DB::DiskCubeFS>(disk_name, path, nullptr, std::move(settings));
    std::cout << "initial cubefs disk successfully " << std::endl;

    EXPECT_EQ(disk_name, cubeFSdisk->getName());
    EXPECT_EQ(path, cubeFSdisk->getPath());


    cubeFSdisk->createDirectory("test_dir1994");
    EXPECT_TRUE(cubeFSdisk->exists("test_dir1994"));
    EXPECT_TRUE(cubeFSdisk->isDirectory("test_dir1994"));
    EXPECT_FALSE(cubeFSdisk->isFile("test_dir1994"));


    cubeFSdisk->createDirectories("test_dir199/test88");
    EXPECT_TRUE(cubeFSdisk->isDirectory("test_dir199/test88"));


    cubeFSdisk->moveDirectory("test_dir199/test88", "test_dir199/test887");
    EXPECT_TRUE(cubeFSdisk->isDirectory("test_dir199/test887"));

    cubeFSdisk->createFile("test_dir199/test887" + file_name);
    EXPECT_TRUE(cubeFSdisk->isFile("test_dir199/test887" + file_name));
    EXPECT_FALSE(cubeFSdisk->isDirectory("test_dir199/test887" + file_name));
}


#endif
