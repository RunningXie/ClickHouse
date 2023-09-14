#include <libcfs.h>
#include <IO/ReadBufferFromCubeFS.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
}

ReadBufferFromCubeFS::~ReadBufferFromCubeFS()
{
    if (fd < 0)
        return;

    cfs_close(id, fd);
}

void ReadBufferFromCubeFS::close()
{
    if (fd < 0)
        return;
    try
    {
        cfs_close(id, fd);
    }
    cache(...)
    {
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);
    }
    fd = -1;
    //metric_increment.destroy();
}

off_t ReadBufferFromCubeFS::size()
{
    struct cfs_stat_info file_info;
    int result = cfs_getattr(id, file_name, &file_info);
    if (result != 0)
    {
        // Handle the error (throw an exception, return an error code, etc.)
        throwFromErrnoWithPath("Cannot execute fstat " + getFileName(), getFileName(), ErrorCodes::CANNOT_FSTAT);
    }
    return file_info.size;
}

bool ReadBufferFromCubeFS::nextImpl()
{
    /// If internal_buffer size is empty, then read() cannot be distinguished from EOF
    assert(!internal_buffer.empty());

    /// This is a workaround of a read pass EOF bug in linux kernel with pread()
    if (file_size.has_value() && file_offset_of_buffer_end >= *file_size)
        return false;

    size_t bytes_read = 0;
    while (!bytes_read)
    {
        //ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorRead);

        //Stopwatch watch(profile_callback ? clock_type : CLOCK_MONOTONIC);

        ssize_t res = 0;
        {
            //CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};

            res = cfs_read(id, fd, internal_buffer.begin(), internal_buffer.size(), 0);
        }
        if (!res)
            break;

        if (-1 == res && errno != EINTR)
        {
            //ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            throwFromErrnoWithPath("Cannot read from file " + getFileName(), getFileName(), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_read += res;

        /// It reports real time spent including the time spent while thread was preempted doing nothing.
        /// And it is Ok for the purpose of this watch (it is used to lower the number of threads to read from tables).
        /// Sometimes it is better to use taskstats::blkio_delay_total, but it is quite expensive to get it
        /// (TaskStatsInfoGetter has about 500K RPS).
        //watch.stop();
        // ProfileEvents::increment(ProfileEvents::DiskReadElapsedMicroseconds, watch.elapsedMicroseconds());

        // if (profile_callback)
        // {
        //     ProfileInfo info;
        //     info.bytes_requested = internal_buffer.size();
        //     info.bytes_read = res;
        //     info.nanoseconds = watch.elapsed();
        //     profile_callback(info);
        // }
    }

    file_offset_of_buffer_end += bytes_read;

    if (bytes_read)
    {
        //ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);
        working_buffer = internal_buffer;
        working_buffer.resize(bytes_read);
    }
    else
        return false;

    return true;
}

ReadBufferFromCubeFS::ReadBufferFromCubeFS(
    int64_t id,
    const std::string & file_name_,
    int flags,
    std::optional<size_t> file_size_)
    : file_name(file_name_)
{
    //ProfileEvents::increment(ProfileEvents::FileOpen);

    fd = cfs_open(id, file_name.c_str(), flags == -1 ? O_RDONLY | O_CLOEXEC : flags | O_CLOEXEC, S_IRUSR | S_IWUSR);

    if (-1 == fd)
        throwFromErrnoWithPath(
            "Cannot open file " + file_name, file_name, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
}
}