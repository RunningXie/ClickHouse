#include "DiskCubeFS.h"

DiskLocal::DiskLocal(const String & name_, const String & path_, ContextPtr context_, SettingsPtr settings_)
    : name(name_), disk_path(path_), logger(&Poco::Logger::get("DiskLocal"), settings(settings_))
{
}

DiskCubeFSSettings : DiskCubeFSSettings(
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
    struct cfs_stat_info stat;
    if (cfs_getattr(id, const_cast<char *>(path.c_str()), &stat) == 0)
    {
        if (stat.uid == geteuid())
            return (stat.mode & S_IRUSR) != 0;
        else if (stat.gid == getegid())
            return (stat.mode & S_IRGRP) != 0;
        else
            return (stat.mode & S_IROTH) != 0 || geteuid() == 0;
    }
    DB::throwFromErrnoWithPath("Cannot check read access to file: " + path, path, DB::ErrorCodes::PATH_ACCESS_DENIED);
}

bool DiskCubeFS::exists(const String & path) const
{
    String full_path = disk_path + "/" + path;
    int result = cfs_getattr(id, const_cast<char *>(full_path.c_str()), &stat);
    return (result == 0);
}

std::optional<size_t> DiskCubeFS::fileSizeSafe(const fs::path & path)
{
    struct cfs_stat_info stat;
    int result = cfs_getattr(id, const_cast<char *>(path.c_str()), &stat);

    if (result == 0)
    {
        return stat.st_size;
    }
    else if (result == -ENOENT || result == -ENOTDIR)
    {
        return std::nullopt;
    }
    else
    {
        // 抛出适当的异常或错误处理
        throw std::runtime_error("Failed to get file size: " + path);
    }
}

std::unique_ptr<ReadBufferFromFileBase> DiskCubeFS::readFile(
    const String & path, const ReadSettings & settings, std::optional<size_t> read_hint, std::optional<size_t> file_size) const
{
    return std::make_unique<ReadBufferFromCubeFS>(settings->id, fs::path(disk_path) / path, flags);
}

std::optional<UInt32> DiskCubeFS::readDiskCheckerMagicNumber() const noexcept
{
    try
    {
        ReadSettings read_settings;
        auto buf = readFile(disk_checker_path, read_settings, {}, {});
        UInt32 magic_number;
        readIntBinary(magic_number, *buf);
        if (buf->eof())
            return magic_number;
        LOG_WARNING(logger, "The size of disk check magic number is more than 4 bytes. Mark it as read failure");
        return {};
    }
    catch (...)
    {
        tryLogCurrentException(
            logger, fmt::format("Cannot read correct disk check magic number from from {}{}", disk_path, disk_checker_path));
        return {};
    }
}

std::unique_ptr<WriteBufferFromFileBase> DiskCubeFS::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    int flags = (mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    return std::make_unique<WriteBufferFromCubeFS>(settings->id, fs::path(disk_path) / path, buf_size, flags);
}

bool DiskCubeFS::setup()
{
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP;
    int result = cfs_mkdirs(id, disk_path, mode);
    if (result != 0)
    {
        LOG_ERROR(logger, "Cannot create the directory of disk {} ({}).", name, disk_path);
        throw;
    }
    try
    {
        if (!canRead(disk_path))
            throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "There is no read access to disk {} ({}).", name, disk_path);
    }
    catch (...)
    {
        LOG_ERROR(logger, "Cannot gain read access of the disk directory: {}", disk_path);
        throw;
    }

    /// If disk checker is disabled, just assume RW by default.
    if (!disk_checker)
        return true;

    try
    {
        if (exists(disk_checker_path))
        {
            auto magic_number = readDiskCheckerMagicNumber();
            if (magic_number)
                disk_checker_magic_number = *magic_number;
            else
            {
                /// The checker file is incorrect. Mark the magic number to uninitialized and try to generate a new checker file.
                disk_checker_magic_number = -1;
            }
        }
    }
    catch (...)
    {
        LOG_ERROR(logger, "We cannot tell if {} exists anymore, or read from it. Most likely disk {} is broken", disk_checker_path, name);
        throw;
    }

    /// Try to create a new checker file. The disk status can be either broken or readonly.
    if (disk_checker_magic_number == -1)
        try
        {
            pcg32_fast rng(randomSeed());
            UInt32 magic_number = rng();
            {
                auto buf = writeFile(disk_checker_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
                writeIntBinary(magic_number, *buf);
            }
            disk_checker_magic_number = magic_number;
        }
        catch (...)
        {
            LOG_WARNING(
                logger,
                "Cannot create/write to {0}. Disk {1} is either readonly or broken. Without setting up disk checker file, "
                "DiskLocalCheckThread "
                "will not be started. Disk is assumed to be RW. Try manually fix the disk and do `SYSTEM RESTART DISK {1}`",
                disk_checker_path,
                name);
            disk_checker_can_check_read = false;
            return true;
        }

    if (disk_checker_magic_number == -1)
        throw Exception("disk_checker_magic_number is not initialized. It's a bug", ErrorCodes::LOGICAL_ERROR);
    return true;
}

void DiskLocal::startup()
{
    try
    {
        broken = false;
        disk_checker_magic_number = -1;
        disk_checker_can_check_read = true;
        readonly = !setup();
    }
    catch (...)
    {
        tryLogCurrentException(logger, fmt::format("Disk {} is marked as broken during startup", name));
        broken = true;
        /// Disk checker is disabled when failing to start up.
        disk_checker_can_check_read = false;
    }
    if (disk_checker && disk_checker_can_check_read)
        disk_checker->startup();
}