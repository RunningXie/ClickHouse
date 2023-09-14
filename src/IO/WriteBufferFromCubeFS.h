#pragma once

#include <IO/WriteBufferFromFileBase.h>

namespace Poco
{
class Logger;
}
namespace DB
{
class WriteBufferFromCubeFS : public WriteBufferFromFileBase
{
public:
    explicit WriteBufferFromCubeFS(
        int64_t id,
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        mode_t mode = 0666,
        char * existing_memory = nullptr,
        size_t alignment = 0);
    ~WriteBufferFromCubeFS() override;
    void close();
    void sync() override;
    void truncate(off_t length); // NOLINT
    std::string getFileName() const override { return file_name; }
    off_t size();
    Poco::Logger * log = &Poco::Logger::get("WriteBufferFromCubeFS");

private:
    void finalizeImpl() override;
    void nextImpl() override;
    int64_t id;
    std::string file_name;
}
}