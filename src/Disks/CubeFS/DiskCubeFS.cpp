#include "DiskCubeFS.h"
#include <IO/ReadBufferFromCubeFS.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromCubeFS.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/filesystemHelpers.h>

namespace CurrentMetrics
{
extern const Metric DiskSpaceReservedForMerge;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int PATH_ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int CANNOT_UNLINK;
    extern const int CANNOT_RMDIR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CREATE_DIRECTORY;
    extern const int CANNOT_STATVFS;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_CLOSE_FILE;
    extern const int ATOMIC_RENAME_FAIL;
    extern const int FILE_ALREADY_EXISTS;
    extern const int DIRECTORY_DOESNT_EXIST;
}

constexpr uint32_t AttrModifyTime = 1 << 3;
constexpr uint32_t AttrAccessTime = 1 << 4;
std::mutex DiskCubeFS::reservation_mutex;
using DiskCubeFSPtr = std::shared_ptr<DiskCubeFS>;

class DiskCubeFSReservation : public IReservation
{
public:
    DiskCubeFSReservation(const DiskCubeFSPtr & disk_, UInt64 size_)
        : disk(disk_), size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {
    }

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk(size_t i) const override
    {
        if (i != 0)
            throw Exception("Can't use i != 0 with single disk reservation. It's a bug", ErrorCodes::LOGICAL_ERROR);
        return disk;
    }

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override
    {
        std::lock_guard lock(DiskCubeFS::reservation_mutex);
        disk->reserved_bytes -= size;
        size = new_size;
        disk->reserved_bytes += size;
    }

    ~DiskCubeFSReservation() override
    {
        try
        {
            std::lock_guard lock(DiskCubeFS::reservation_mutex);
            if (disk->reserved_bytes < size)
            {
                disk->reserved_bytes = 0;
                LOG_ERROR(&Poco::Logger::get("DiskCubeFS"), "Unbalanced reservations size for disk '{}'.", disk->getName());
            }
            else
            {
                disk->reserved_bytes -= size;
            }

            if (disk->reservation_count == 0)
                LOG_ERROR(&Poco::Logger::get("DiskCubeFS"), "Unbalanced reservation count for disk '{}'.", disk->getName());
            else
                --disk->reservation_count;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    DiskCubeFSPtr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};


ReservationPtr DiskCubeFS::reserve(UInt64 bytes)
{
    if (!tryReserve(bytes))
        return {};
    return std::make_unique<DiskCubeFSReservation>(std::static_pointer_cast<DiskCubeFS>(shared_from_this()), bytes);
}

bool DiskCubeFS::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(DiskCubeFS::reservation_mutex);
    if (bytes == 0)
    {
        LOG_DEBUG(logger, "Reserving 0 bytes on disk {}", backQuote(name));
        ++reservation_count;
        return true;
    }

    auto available_space = getTotalSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);
    if (unreserved_space >= bytes)
    {
        LOG_DEBUG(
            logger, "Reserving {} on disk {}, having unreserved {}.", ReadableSize(bytes), backQuote(name), ReadableSize(unreserved_space));
        ++reservation_count;
        reserved_bytes += bytes;
        return true;
    }
    return false;
}

UInt64 DiskCubeFS::getTotalSpace() const
{
    if (broken || readonly)
        return 0;

    UInt64 total_size = static_cast<UInt64>(getFileSize(""));
    if (total_size < keep_free_space_bytes)
        return 0;
    return total_size - keep_free_space_bytes;
}

UInt64 DiskCubeFS::getAvailableSpace() const
{
    return getTotalSpace();
}

UInt64 DiskCubeFS::getUnreservedSpace() const
{
    std::lock_guard lock(DiskCubeFS::reservation_mutex);
    auto available_space = getTotalSpace();
    available_space -= std::min(available_space, reserved_bytes);
    return available_space;
}

bool DiskCubeFS::isFile(const String & path) const
{
    cfs_stat_info stat = getFileAttributes(path);

    // 检查文件类型
    if (S_ISREG(stat.mode))
        return true;

    return false;
}

bool DiskCubeFS::isDirectory(const String & path) const
{
    // 获取文件属性
    cfs_stat_info stat = getFileAttributes(path);

    // 检查文件类型
    if (S_ISDIR(stat.mode))
        return true;

    return false;
}

size_t DiskCubeFS::getFileSize(const String & path) const
{
    // 获取文件属性
    cfs_stat_info stat = getFileAttributes(path);

    // 返回文件大小
    return stat.size;
}

cfs_stat_info DiskCubeFS::getFileAttributes(const String & relative_path) const
{
    cfs_stat_info stat;
    fs::path full_path = fs::path(disk_path) / relative_path;
    int result = cfs_getattr(settings->id, const_cast<char *>(full_path.c_str()), &stat);
    if (result != 0)
    {
        int error_code = errno;
        std::string error_message = strerror(error_code);
        std::string full_error_message = "Failed to get file attribute: " + full_path.string() + ": " + error_message;
        throwFromErrnoWithPath(full_error_message, full_path, ErrorCodes::CANNOT_STATVFS);
    }
    return stat;
}

void DiskCubeFS::createDirectory(const String & path)
{
    createDirectories(path);
}

void DiskCubeFS::createDirectories(const String & path)
{
    fs::path full_path = fs::path(disk_path) / path;
    std::cout << "client id: " << settings->id << std::endl;
    std::cout << "path: " << const_cast<char *>(full_path.string().c_str()) << std::endl;
    int result = cfs_mkdirs(settings->id, const_cast<char *>(full_path.string().c_str()), O_RDWR | O_CREAT);
    if (result != 0)
    {
        // 处理创建目录失败的情况
        // 返回适当的错误码或执行其他操作
        throwFromErrnoWithPath("Cannot mkdir: " + full_path.string(), full_path, ErrorCodes::CANNOT_CREATE_DIRECTORY);
    }
}

void DiskCubeFS::clearDirectory(const String & path)
{
    std::vector<String> file_names;
    listFiles(path, file_names);
    for (const auto & filename : file_names)
    {
        fs::path file_path = fs::path(disk_path) / filename;

        int result = cfs_unlink(settings->id, const_cast<char *>(file_path.string().c_str()));
        if (result < 0)
        {
            throwFromErrnoWithPath("Cannot unlink file: " + file_path.string(), file_path, ErrorCodes::CANNOT_UNLINK);
        }
    }
}

void DiskCubeFS::listFiles(const String & path, std::vector<String> & file_names)
{
    file_names.clear();
    fs::path full_path = (fs::path(disk_path) / path);
    std::cout << "list full path: " << full_path.string() << std::endl;
    int fd = -1;
    fd = cfs_open(settings->id, const_cast<char *>(full_path.string().c_str()), 0, 0);
    if (fd < 0)
    {
        throwFromErrnoWithPath("Cannot open: " + full_path.string(), full_path, ErrorCodes::CANNOT_OPEN_FILE);
    }
    int count = 10000;
    while (true)
    {
        std::vector<cfs_dirent> direntsInfo(count);

        std::memset(direntsInfo.data(), 0, count * sizeof(cfs_dirent_info));


        int num_entries = cfs_readdir(settings->id, fd, {direntsInfo.data(), count, count}, count);
        if (num_entries < 0)
        {
            throwFromErrnoWithPath("Cannot readdir: " + full_path.string(), full_path, ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }
        std::cout << "readdir result, num_entries: " << num_entries << std::endl;
        if (num_entries < count)
        {
            for (int i = 0; i < num_entries; i++)
            {
                std::string file_name(direntsInfo[i].name);
                std::cout << "filename: " << file_name << std::endl;
                file_names.emplace_back(file_name);
            }
            cfs_close(settings->id, fd);
            return;
        }
        count = count * 2;
    }
}


void DiskCubeFS::removeFile(const String & path)
{
    fs::path full_path = (fs::path(disk_path) / path);
    int result = cfs_unlink(settings->id, const_cast<char *>(full_path.string().c_str()));

    if (result != 0)
    {
        // 根据需要进行错误处理
        throwFromErrnoWithPath("Cannot unlink file: " + full_path.string(), full_path, ErrorCodes::CANNOT_UNLINK);
    }
}

void DiskCubeFS::moveDirectory(const String & from_path, const String & to_path)
{
    moveFile(from_path, to_path);
}

class DiskCubeFSDirectoryIterator final : public IDiskDirectoryIterator
{
public:
    DiskCubeFSDirectoryIterator() : id(-1), fd(-1), current_index(0) { }
    DiskCubeFSDirectoryIterator(int64_t client_id, const String & path) : id(client_id), dir_path(path), fd(-1), current_index(0)
    {
        openDirectory();
    }

    ~DiskCubeFSDirectoryIterator() override { closeDirectory(); }

    void next() override { ++current_index; }

    bool isValid() const override { return current_index < dirents.size(); }

    String path() const override
    {
        if (isValid())
            return dir_path + "/" + dirents[current_index];
        else
            return "";
    }

    String name() const override
    {
        if (isValid())
            return dirents[current_index];
        else
            return "";
    }

private:
    int64_t id;
    String dir_path;
    int fd;
    std::vector<String> dirents;
    size_t current_index;

    void openDirectory()
    {
        fd = cfs_open(id, const_cast<char *>(dir_path.c_str()), O_RDONLY | O_DIRECTORY, 0755);
        if (fd < 0)
        {
            throwFromErrnoWithPath("Cannot open: " + dir_path, fs::path(dir_path), ErrorCodes::CANNOT_OPEN_FILE);
        }
        GoSlice dirents_slice;
        int result = cfs_readdir(id, fd, dirents_slice, 0);
        if (result < 0)
        {
            throwFromErrnoWithPath("Cannot readdir: " + dir_path, fs::path(dir_path), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }
        dirents.resize(dirents_slice.len);
        for (auto i = 0; i < dirents_slice.len; ++i)
        {
            char * entry = static_cast<char *>(dirents_slice.data);
            dirents[i] = entry;
        }
    }

    void closeDirectory()
    {
        if (fd < 0)
            return;
        cfs_close(id, fd);
    }
};

DiskDirectoryIteratorPtr DiskCubeFS::iterateDirectory(const String & path)
{
    fs::path meta_path = fs::path(disk_path) / path;
    if (!broken && exists(meta_path) && isDirectory(meta_path))
        return std::make_unique<DiskCubeFSDirectoryIterator>(settings->id, meta_path.string());
    else
        return std::make_unique<DiskCubeFSDirectoryIterator>();
}

void DiskCubeFS::createFile(const String & path)
{
    fs::path full_path = fs::path(disk_path) / path;
    fs::path parent_dir = full_path.parent_path();
    if (!exists(parent_dir))
    {
        // 上级目录不存在，可以选择抛出异常或执行其他逻辑
        throwFromErrnoWithPath("Parent directory not exist", parent_dir, ErrorCodes::DIRECTORY_DOESNT_EXIST);
    }
    int fd = cfs_open(
        settings->id,
        const_cast<char *>(full_path.string().c_str()),
        O_WRONLY | O_CREAT | O_EXCL,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (fd < 0)
    {
        throwFromErrnoWithPath("Cannot open: " + full_path.string(), full_path, ErrorCodes::CANNOT_OPEN_FILE);
    }
    cfs_close(settings->id, fd);
}

void DiskCubeFS::replaceFile(const String & from_path, const String & to_path)
{
    fs::path full_from_path = fs::path(disk_path) / from_path;
    fs::path full_to_path = fs::path(disk_path) / to_path;
    // 检查目标路径的上级目录是否存在
    fs::path parent_dir = full_to_path.parent_path();
    if (!exists(parent_dir))
    {
        // 上级目录不存在，可以选择抛出异常或执行其他逻辑
        throwFromErrnoWithPath("Destination directory not exist", parent_dir, ErrorCodes::DIRECTORY_DOESNT_EXIST);
    }
    int result = cfs_rename(settings->id, const_cast<char *>(full_from_path.string().c_str()), const_cast<char *>(full_to_path.string().c_str()));
    if (result != 0)
    {
         throwFromErrnoWithPath(
             "Failed to rename file, from path: " + full_from_path.string() + " to path: " + full_to_path.string(),
             "",
             ErrorCodes::ATOMIC_RENAME_FAIL);
    }
}

void DiskCubeFS::moveFile(const String & from_path, const String & to_path)
{
    // 检查目标路径是否存在文件
    if (exists(to_path))
    {
        throwFromErrnoWithPath("Destination file: " + (fs::path(disk_path) / to_path).string() + "already exists.",fs::path(disk_path) / to_path, 
        ErrorCodes::FILE_ALREADY_EXISTS);
    }
    replaceFile(from_path, to_path);
}

void DiskCubeFS::removeFileIfExists(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != cfs_unlink(settings->id, const_cast<char *>(fs_path.c_str())) && errno != ENOENT)
        throwFromErrnoWithPath("Cannot unlink file " + fs_path.string(), fs_path, ErrorCodes::CANNOT_UNLINK);
}

void DiskCubeFS::removeDirectory(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != cfs_rmdir(settings->id, const_cast<char *>(fs_path.c_str())))
    {
        int error_code = errno;
        std::string error_message = strerror(error_code);
        std::string full_error_message = "Cannot rmdir " + fs_path.string() + ": " + error_message;
        throwFromErrnoWithPath(full_error_message, fs_path, ErrorCodes::CANNOT_RMDIR);
    }
}

void DiskCubeFS::removeRecursive(const String & path)
{
    std::cout << "remove recursive, handle relative path: " << path << std::endl;
    cfs_stat_info stat = getFileAttributes(path);
    if (S_ISDIR(stat.mode))
    {
        std::vector<String> file_names;
        listFiles(path, file_names);
        std::cout << "listFiles result: " << std::endl;
        for (const auto & filename : file_names)
        {
            std::cout << filename << std::endl;
        }

for (const auto & filename : file_names)
{
            auto child_path = fs::path(path) / filename;
            removeRecursive(child_path.string());
}
        // 删除空目录
        removeDirectory(path);
}
    else
    {
        // 文件
        removeFile(path);
    }
}

void DiskCubeFS::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    FS::setModificationTime(fs::path(disk_path) / path, timestamp.epochTime());
    cfs_stat_info stat = getFileAttributes(path);
    stat.atime = timestamp.epochTime();
    stat.mtime = timestamp.epochTime();
    // 使用 cfs_setattr 函数来设置新的文件属性
    fs::path full_path = fs::path(disk_path) / path;
    if (cfs_setattr(settings->id, const_cast<char *>(full_path.string().c_str()), &stat, AttrModifyTime | AttrAccessTime) != 0)
    {
        throwFromErrnoWithPath("Cannot setattr " + full_path.string(), full_path, ErrorCodes::LOGICAL_ERROR);
    }
}

Poco::Timestamp DiskCubeFS::getLastModified(const String & path)
{
    return Poco::Timestamp::fromEpochTime(getLastChanged(path));
}

time_t DiskCubeFS::getLastChanged(const String & path) const
{
    cfs_stat_info stat = getFileAttributes(path);
    return stat.ctime;
}

void DiskCubeFS::setReadOnly(const String & path)
{
    cfs_stat_info stat = getFileAttributes(path);
    fs::path full_path = fs::path(disk_path) / path;
    int fd = cfs_open(settings->id, const_cast<char *>(full_path.string().c_str()), 0, 0);
    if (fd < 0)
    {
        throwFromErrnoWithPath("Cannot open: " + full_path.string(), full_path, ErrorCodes::CANNOT_OPEN_FILE);
    }
    // 设置只读权限
    mode_t newMode = stat.mode & ~(S_IWUSR | S_IWGRP | S_IWOTH);
    // 使用 cfs_chmod 函数来更改文件权限
    if (cfs_fchmod(settings->id, fd, newMode) != 0)
    {
        throwFromErrnoWithPath("Cannot fchmod " + full_path.string(), full_path, ErrorCodes::LOGICAL_ERROR);
    }
}

void DiskCubeFS::createHardLink(const String &, const String &)
{
    LOG_DEBUG(logger, "Need create hard link function!");
    // fs::path full_src_path = fs::path(disk_path) / src_path;
    // fs::path full_dst_path = fs::path(disk_path) / dst_path;
    // // 使用 cfs_link 函数来创建硬链接
    // if (cfs_link(settings->id, const_cast<char *>(full_src_path.string().c_str()), const_cast<char *>(full_dst_path.string().c_str())) != 0)
    // {
    //     auto link_errno = errno;
    //     if (errno == EEXIST)
    //     {
    //         // 目标链接已存在，进行进一步的检查
    //         cfs_stat_info source_stat = getFileAttributes(src_path);
    //         cfs_stat_info destination_stat = getFileAttributes(dst_path);
    //         // 检查源文件和目标链接的 inode 是否相同
    //         if (source_stat.ino != destination_stat.ino)
    //         {
    //             throwFromErrnoWithPath(
    //                 "Destination file " + destination_path.string() + " is already exist and have different inode.",
    //                 destination_path,
    //                 ErrorCodes::CANNOT_LINK,
    //                 link_errno);
    //         }
    //     }
    //     else
    //     {
    //         throwFromErrnoWithPath(
    //             "Cannot link " + full_src_path.string() + " to " + full_dst_path.string(), destination_path, ErrorCodes::CANNOT_LINK);
    //     }
    // }
}

DiskCubeFS::DiskCubeFS(const String & name_, const String & path_, SettingsPtr settings_)
    : name(name_), disk_path(path_), settings(std::move(settings_)), logger(&Poco::Logger::get("DiskCubeFS"))
{
    std::cout << "client id: " << settings->id << std::endl;
    if (cfs_start_client(settings->id) != 0)
    {
        throwFromErrnoWithPath("Start cfs client failed", "", ErrorCodes::LOGICAL_ERROR);
    }
}

DiskCubeFS::DiskCubeFS(const String & name_, const String & path_, ContextPtr, SettingsPtr settings_)
    : name(name_), disk_path(path_), settings(std::move(settings_)), logger(&Poco::Logger::get("DiskCubeFS"))
{
    std::cout << "client id: " << settings->id << std::endl;
    if (cfs_start_client(settings->id) != 0)
    {
        throwFromErrnoWithPath("Start cfs client failed", "", ErrorCodes::LOGICAL_ERROR);
    }
}

DiskCubeFSSettings ::DiskCubeFSSettings(
    const int64_t id_,
    String vol_name_,
    String master_addr_,
    String log_dir_,
    String log_level_,
    String access_key_,
    String secret_key_,
    String push_addr_)
    : id(id_)
    , vol_name(vol_name_)
    , master_addr(master_addr_)
    , log_dir(log_dir_)
    , log_level(log_level_)
    , access_key(access_key_)
    , secret_key(secret_key_)
    , push_addr(push_addr_)
{
}

bool DiskCubeFS::canRead(const std::string & path)
{
    cfs_stat_info stat = getFileAttributes(path);
    if (stat.uid == geteuid())
        return (stat.mode & S_IRUSR) != 0;
    else if (stat.gid == getegid())
        return (stat.mode & S_IRGRP) != 0;
    else
        return (stat.mode & S_IROTH) != 0 || geteuid() == 0;
}

bool DiskCubeFS::exists(const String & path) const
{
    String full_path = disk_path + "/" + path;
    cfs_stat_info stat;
    int result = cfs_getattr(settings->id, const_cast<char *>(full_path.c_str()), &stat);
    return (result == 0);
}

std::optional<size_t> DiskCubeFS::fileSizeSafe(const fs::path & path)
{
    cfs_stat_info stat;
    int result = cfs_getattr(settings->id, const_cast<char *>(path.c_str()), &stat);

    if (result == 0)
    {
        return stat.size;
    }
    else if (result == -ENOENT || result == -ENOTDIR)
    {
        return std::nullopt;
    }
    else
    {
        // 抛出适当的异常或错误处理
        throw std::runtime_error("Failed to get file size: " + path.string());
    }
}

std::unique_ptr<ReadBufferFromFileBase>
DiskCubeFS::readFile(const String & path, const ReadSettings &, std::optional<size_t>, std::optional<size_t>) const
{
    return std::make_unique<ReadBufferFromCubeFS>(settings->id, fs::path(disk_path) / path, O_RDONLY | O_CLOEXEC);
}

// std::optional<UInt32> DiskCubeFS::readDiskCheckerMagicNumber() const noexcept
// {
//     try
//     {
//         auto buf = readFile(disk_checker_path);
//         UInt32 magic_number;
//         readIntBinary(magic_number, *buf);
//         if (buf->eof())
//             return magic_number;
//         LOG_WARNING(logger, "The size of disk check magic number is more than 4 bytes. Mark it as read failure");
//         return {};
//     }
//     catch (...)
//     {
//         tryLogCurrentException(
//             logger, fmt::format("Cannot read correct disk check magic number from from {}{}", disk_path, disk_checker_path));
//         return {};
//     }
// }

std::unique_ptr<WriteBufferFromFileBase> DiskCubeFS::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    int flags = (mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    return std::make_unique<WriteBufferFromCubeFS>(settings->id, fs::path(disk_path) / path, buf_size, flags);
}
}
