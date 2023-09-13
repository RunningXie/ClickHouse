#pragma once

#include <IO/ReadBufferFromFileBase.h>


namespace Poco
{
class Logger;
}
namespace DB
{
class ReadBufferFromCubeFS : public ReadBufferFromFileBase
{
public:
    explicit ReadBufferFromFile(
        int64_t id_ const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt);
    ~ReadBufferFromFile() override;
    std::string getFileName() const override { return file_name; }
    Range getRemainingReadRange() const override { return Range{.left = file_offset_of_buffer_end, .right = std::nullopt}; }
    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end; }
    void close();
    /** Could be used before initialization if needed 'fd' was not passed to constructor.
      * It's not possible to change 'fd' during work.
      */
    void setFD(int fd_) { fd = fd_; }
    int getFD() const { return fd; }
    off_t size() const;
    Poco::Logger * log = &Poco::Logger::get("WriteBufferFromCubeFS");


private:
    void nextImpl() override;
    int fd;
    int64_t id;
    std::string file_name;
};
}
