#include <string>
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
const String disk_name = "cubefs";


class DiskTestCubeFS : public testing::Test
{
public:
    void SetUp() override
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
        disk = std::make_shared<DB::DiskCubeFS>(disk_name, path, nullptr, std::move(settings));
        std::cout << "initial cubefs disk successfully " << std::endl;
    }
    void TearDown() override
    {
        disk->removeRecursive("");
        disk.reset();
    }

    DB::DiskPtr disk;
};

TEST_F(DiskTestCubeFS, createDirectory)
{
EXPECT_TRUE(this->disk->exists("/"));
std::cout<<"empty exist is ok"<<std::endl;
    EXPECT_EQ(disk_name, this->disk->getName());

    disk->createDirectory("create_directory");
    EXPECT_TRUE(this->disk->exists("create_directory"));
    EXPECT_TRUE(this->disk->isDirectory("create_directory"));
    EXPECT_FALSE(this->disk->isFile("create_directory"));

    this->disk->createDirectories("not_exist_parent_directory/subdirectory");
    EXPECT_TRUE(this->disk->isDirectory("not_exist_parent_directory/subdirectory"));
    std::cout << "class disk path: " << disk->getPath() << std::endl;
}


TEST_F(DiskTestCubeFS, moveDirectory)
{
disk->createDirectory("create_directory");
    disk->createDirectories("not_exist_parent_directory/subdirectory");
    try
    {
        std::cout << "try move directory" << std::endl;
        this->disk->moveDirectory("not_exist_parent_directory/subdirectory", "not_exist/create_directory/");
        FAIL() << "Expected exception to be thrown.";
        //EXPECT_THROW(cubeFSdisk->moveDirectory("not_exist_parent_directory/subdirectory", "not_exist/create_directory/"), std::exception);
    }
    catch (const std::exception & e)
    {
        std::string errorMessage = e.what();
        std::string expectedSubstring = "Destination directory does not exist";
        EXPECT_TRUE(errorMessage.find(expectedSubstring) != std::string::npos);
    }
    std::cout << "try move directory end" << std::endl;
    this->disk->moveDirectory("not_exist_parent_directory/subdirectory", "create_directory/subdirectory");
    EXPECT_TRUE(this->disk->isDirectory("create_directory/subdirectory"));
    std::cout << "move directory end" << std::endl;
}

TEST_F(DiskTestCubeFS, createFile)
{
    try
    {
disk->createFile("create_directory/file");
FAIL() << "Expected exception to be thrown.";
    }
    catch (const std::exception & e)
    {
std::string errorMessage = e.what();
std::string expectedSubstring = "Parent directory does not exist";
EXPECT_TRUE(errorMessage.find(expectedSubstring) != std::string::npos);
    }
    disk->createDirectory("create_directory");

    this->disk->createFile("create_directory/file");
    EXPECT_TRUE(this->disk->isFile("create_directory/file"));
    EXPECT_FALSE(this->disk->isDirectory("create_directory/file"));
    std::cout << "create file end" << std::endl;
}

TEST_F(DiskTestCubeFS, moveFile)
{
    disk->createDirectory("create_directory");
    disk->createFile("create_directory/file");
    this->disk->moveDirectory("create_directory", "move_directory");
    EXPECT_TRUE(this->disk->isFile("move_directory/file"));
    std::cout << "move directory end" << std::endl;
    this->disk->moveFile("move_directory/file", "move_file");
    EXPECT_TRUE(this->disk->isFile("move_file"));
    std::cout << "move file end" << std::endl;
    disk->createDirectory("create_directory");
    this->disk->createFile("create_directory/create_file");
    std::cout << "create file end" << std::endl;
    try
    {
        std::cout << "try move file" << std::endl;
        //   EXPECT_THROW(cubeFSdisk->moveFile("move_file", "create_directory/create_file"), std::exception);
        this->disk->moveFile("move_file", "create_directory/create_file");
        FAIL() << "Expected exception to be thrown.";
    }
    catch (const std::exception & e)
    {
        std::string errorMessage = e.what();
        std::string expectedSubstring = "Destination directory does not exist";
        EXPECT_TRUE(errorMessage.find(expectedSubstring) != std::string::npos);

        //EXPECT_THAT(e.what(), testing::HasSubstr("already exists"));
        //std::cout<<e.what()<<std::endl;
        //EXPECT_EQ(e.code(), DB::ErrorCodes::FILE_ALREADY_EXISTS);
    }
    disk->replaceFile("move_file", "create_directory/create_file");
    EXPECT_TRUE(this->disk->isFile("create_directory/create_file"));
}

#endif
