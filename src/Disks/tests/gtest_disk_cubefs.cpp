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
      disk->cfs_close_client(settings->id);
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
    disk->createDirectory("create_directory");
    this->disk->createFile("create_directory/create_file");
    try
    {
this->disk->moveFile("move_file", "create_directory/create_file");
FAIL() << "Expected exception to be thrown.";
    }
    catch (const std::exception & e)
    {
        std::string errorMessage = e.what();
        std::string expectedSubstring = "Destination file already exists";
        EXPECT_TRUE(errorMessage.find(expectedSubstring) != std::string::npos);
    }
EXPECT_TRUE(disk->exists("create_directory/create_file"));
std::cout<<"start replace file"<<std::endl;
    disk->replaceFile("move_file", "create_directory/create_file");
    EXPECT_TRUE(this->disk->isFile("create_directory/create_file"));
}

TEST_F(DiskTestCubeFS, remove)
{
    try
    {
        disk->removeFile("not_exist");
        FAIL() << "Expected exception to be thrown.";
    }
    catch (const std::exception & e)
    {
        std::string errorMessage = e.what();
        std::string expectedSubstring = "File does not exists";
        EXPECT_TRUE(errorMessage.find(expectedSubstring) != std::string::npos);
    }
    disk->removeFileIfExists("not_exist");
}

TEST_F(DiskTestCubeFS, lastModified)
{
    Poco::Timestamp timestamp; //直接获取当前时间
    disk->createFile("file");
    disk->setLastModified("file", timestamp);
    EXPECT_EQ(timestamp, disk->getLastModified("file"));
}

TEST_F(DiskTest, writeFile)
{
    {
        std::unique_ptr<DB::WriteBuffer> out = this->disk->writeFile("test_file"); //IDisk后两个参数有默认值
        writeString("test data", *out);
    }

    DB::String data;
    {
        std::unique_ptr<DB::ReadBuffer> in = this->disk->readFile("test_file");
        readString(data, *in);
    }

    EXPECT_EQ("test data", data);
    EXPECT_EQ(data.size(), this->disk->getFileSize("test_file"));
}

TEST_F(DiskTestCubeFS, readFile)
{
    try
    {
        std::unique_ptr<DB::ReadBuffer> in = this->disk->readFile("test_file");
        FAIL() << "Expected exception to be thrown.";
    }
    catch (const std::exception & e)
    {
        std::string errorMessage = e.what();
        std::string expectedSubstring = "File does not exists";
        EXPECT_TRUE(errorMessage.find(expectedSubstring) != std::string::npos);
    }

    {
        std::unique_ptr<DB::WriteBuffer> out = this->disk->writeFile("test_file");
        writeString("test data", *out);
    }

    // Test SEEK_SET
    {
        String buf(4, '0');
        in = this->disk->readFile("test_file");

        in->seek(5, SEEK_SET);

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("data", buf);
    }

    // Test SEEK_CUR
    {
        std::unique_ptr<DB::SeekableReadBuffer> in = this->disk->readFile("test_file");
        String buf(4, '0');

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("test", buf);

        // Skip whitespace
        in->seek(1, SEEK_CUR);

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("data", buf);
    }
}

TEST_F(DiskTest, iterateDirectory)
{
    this->disk->createDirectories("test_dir/nested_dir/");

    {
        auto iter = this->disk->iterateDirectory("");
        EXPECT_TRUE(iter->isValid());
        EXPECT_EQ("test_dir/", iter->path());
        iter->next();
        EXPECT_FALSE(iter->isValid());
    }

    {
        auto iter = this->disk->iterateDirectory("test_dir/");
        EXPECT_TRUE(iter->isValid());
        EXPECT_EQ("test_dir/nested_dir/", iter->path());
        iter->next();
        EXPECT_FALSE(iter->isValid());
    }
}

#endif
