#include <libcfs.h>
#include <IO/WriteBufferFromCubeFS.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_FSYNC;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int CANNOT_FSTAT;
}
}

WriteBufferFromCubeFS::WriteBufferFromCubeFS(
    int64_t id_, const std::string & file_name_, size_t buf_size, int flags, mode_t mode, char * existing_memory, size_t alignment)
    : WriteBufferFromFileBase(buf_size, existing_memory, alignment), id(id_), file_name(std::move(file_name_))
{
    fd = cfs_open(
        id, const_cast<char *>(file_name_.data()), flags == -1 ? O_WRONLY | O_TRUNC | O_CREAT | O_CLOEXEC : flags | O_CLOEXEC, mode);

    if (-1 == fd)
        throwFromErrnoWithPath(
            "Cannot open file " + file_name, file_name, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
}

WriteBufferFromCubeFS::~WriteBufferFromCubeFS()
{
    finalize();
    cfs_close(id, fd);
}

void WriteBufferFromCubeFS::close()
{
    if (fd < 0)
        return;
    next();
    cfs_close(id, fd);
    fd = -1;
}

void WriteBufferFromCubeFS::finalizeImpl()
{
    if (fd < 0)
        return;

    next();
}

void WriteBufferFromCubeFS::nextImpl()
{
    if (!offset())
        return;

    //Stopwatch watch;

    size_t bytes_written = 0;
    while (bytes_written != offset())
    {
        //ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWrite);

        ssize_t res = 0;
        {
            //CurrentMetrics::Increment metric_increment{CurrentMetrics::Write};
            res = cfs_write(id, fd, working_buffer.begin() + bytes_written, offset() - bytes_written, 0);
        }

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            //ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteFailed);

            /// Don't use getFileName() here because this method can be called from destructor
            String error_file_name = file_name;
            if (error_file_name.empty())
                error_file_name = "(fd = " + toString(fd) + ")";
            throwFromErrnoWithPath("Cannot write to file " + error_file_name, error_file_name, ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_written += res;
    }

    //ProfileEvents::increment(ProfileEvents::DiskWriteElapsedMicroseconds, watch.elapsedMicroseconds());
    //ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteBytes, bytes_written);
}

void WriteBufferFromCubeFS::sync()
{
    /// If buffer has pending data - write it.
    next();

    int res = cfs_flush(id, fd);
    if (-1 == res)
        throwFromErrnoWithPath("Cannot flush " + getFileName(), getFileName(), ErrorCodes::CANNOT_FSYNC);
}

off_t WriteBufferFromCubeFS::size()
{
    struct cfs_stat_info file_info;
    int result = cfs_getattr(id, const_cast<char *>(file_name.data()), &file_info);
    if (result != 0)
    {
        // Handle the error (throw an exception, return an error code, etc.)
        throwFromErrnoWithPath("Cannot execute fstat " + getFileName(), getFileName(), ErrorCodes::CANNOT_FSTAT);
    }
    return file_info.size;
}
}
