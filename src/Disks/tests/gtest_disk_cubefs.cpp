#include <gtest/gtest.h>
#define RUN_CUBEFS_TEST 1
#if RUN_CUBEFS_TEST

#    include <Disks/CubeFS/DiskCubeFS.h>

namespace DB
{
int64_t getClientId();
}

const String path = "/test_dir";
const String vol_name = "xieyichen";
const String master_addr = "cfs-south.oppo.local";
const String log_dir = "/home/service/var/logs/cfs/test-log";
const String log_level = "debug";
const String access_key = "jRlZO65q7XlH5bnV";
const String secret_key = "V1m730UzREHaK1jCkC0kL0cewOX0kH3K";
const String push_addr = "cfs.dg-push.wanyol.com";
const String file_name = "test.txt";

TEST(DiskTestCubeFS, RemoveFileCubeFS)
{
    auto settings = std::make_unique<DB::DiskCubeFSSettings>(
        DB::getClientId(), vol_name, master_addr, log_dir, log_level, access_key, secret_key, push_addr);
    std::shared_ptr<DB::IDisk> cubeFSdisk = std::make_shared<DB::DiskCubeFS>("cubefs", path, nullptr, settings);
    cubeFSdisk->writeFile(file_name, 1024, DB::WriteMode::Rewrite);
    bool exists_result = cubeFSdisk->exists(file_name);
    EXPECT_EQ(true, exists_result);
}

#endif
