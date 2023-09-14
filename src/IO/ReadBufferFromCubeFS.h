#pragma once

#include <IO/ReadBufferFromFileBase.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_SELECT;
    extern const int CANNOT_FSTAT;
    extern const int CANNOT_ADVISE;
}
class ReadBufferFromCubeFS : public ReadBufferFromFileBase
{
public:
    explicit ReadBufferFromCubeFS(
        int64_t id_, const std::string & file_name_, int flags = -1);
    ~ReadBufferFromCubeFS() override;
    std::string getFileName() const override { return file_name; }
    Range getRemainingReadRange() const override { return Range{.left = file_offset_of_buffer_end, .right = std::nullopt}; }
    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end; }
    off_t getPosition() override { return file_offset_of_buffer_end - (working_buffer.end() - pos); }
    void close();
    /** Could be used before initialization if needed 'fd' was not passed to constructor.
      * It's not possible to change 'fd' during work.
      */
    void setFD(int fd_) { fd = fd_; }
    int getFD() const { return fd; }
    off_t size();

private:
    bool nextImpl() override;
    int fd;
    int64_t id;
    std::string file_name;
    size_t file_offset_of_buffer_end = 0; /// What offset in file corresponds to working_buffer.end().
};
}
