#include <Disks/CubeFS/DiskCubeFS.h>
#include <Disks/CubeFS/DiskCubeFSCheckThread.h>
#include <Interpreters/Context.h>
#include <Common/createHardLink.h>
#include <Disks/DiskFactory.h>

#include <Disks/LocalDirectorySyncGuard.h>
#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>
#include <Common/atomicRename.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Disks/ObjectStorages/LocalObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/FakeMetadataStorageFromDisk.h>

#include <fstream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <Disks/DiskFactory.h>
#include <Common/randomSeed.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

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
    extern const int CANNOT_STAT;
}

std::mutex DiskCubeFS::reservation_mutex;

void loadDiskCubeFSConfig(const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      String & path,
                      UInt64 & keep_free_space_bytes)
{
    path = config.getString(config_prefix + ".path", "");
    if (name == "default")
    {
        if (!path.empty())
            throw Exception(
                "\"default\" disk path should be provided in <path> not it <storage_configuration>",
                ErrorCodes::BAD_ARGUMENTS);
        path = context->getPath();
    }
    else
    {
        if (path.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk path can not be empty. Disk {}", name);
        if (path.back() != '/')
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk path must end with /. Disk {}", name);
        if (path == context->getPath())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk path ('{}') cannot be equal to <path>. Use <default> disk instead.", path);
    }

    bool has_space_ratio = config.has(config_prefix + ".keep_free_space_ratio");

    if (config.has(config_prefix + ".keep_free_space_bytes") && has_space_ratio)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Only one of 'keep_free_space_bytes' and 'keep_free_space_ratio' can be specified");

    keep_free_space_bytes = config.getUInt64(config_prefix + ".keep_free_space_bytes", 0);

}

static std::optional<size_t> fileSizeSafe(const fs::path & path)
{
    std::error_code ec;

    size_t size = fs::file_size(path, ec);
    if (!ec)
        return size;

    if (ec == std::errc::no_such_file_or_directory)
        return std::nullopt;
    if (ec == std::errc::operation_not_supported)
        return std::nullopt;

    throw fs::filesystem_error("DiskCubeFS", path, ec);
}

class DiskCubeFSReservation : public IReservation
{
public:
    DiskCubeFSReservation(const DiskCubeFSPtr & disk_, UInt64 size_, UInt64 unreserved_space_)
        : disk(disk_)
        , size(size_)
        , unreserved_space(unreserved_space_)
        , metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {}

    UInt64 getSize() const override { return size; }
    UInt64 getUnreservedSpace() const override { return unreserved_space; }

    DiskPtr getDisk(size_t i) const override
    {
        if (i != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't use i != 0 with single disk reservation. It's a bug");
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
    UInt64 unreserved_space;
    CurrentMetrics::Increment metric_increment;
};


class DiskCubeFSDirectoryIterator final : public IDirectoryIterator
{
public:
    DiskCubeFSDirectoryIterator() = default;
    DiskCubeFSDirectoryIterator(const String & disk_path_, const String & dir_path_)
        : dir_path(dir_path_), entry(fs::path(disk_path_) / dir_path_)
    {
    }

    void next() override { ++entry; }

    bool isValid() const override { return entry != fs::directory_iterator(); }

    String path() const override
    {
        if (entry->is_directory())
            return dir_path / entry->path().filename() / "";
        else
            return dir_path / entry->path().filename();
    }


    String name() const override { return entry->path().filename(); }

private:
    fs::path dir_path;
    fs::directory_iterator entry;
};


ReservationPtr DiskCubeFS::reserve(UInt64 bytes)
{
    auto unreserved_space = tryReserve(bytes);
    if (!unreserved_space.has_value())
        return {};
    return std::make_unique<DiskCubeFSReservation>(
        std::static_pointer_cast<DiskCubeFS>(shared_from_this()),
        bytes, unreserved_space.value());
}

std::optional<UInt64> DiskCubeFS::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(DiskCubeFS::reservation_mutex);

    UInt64 available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);

    if (bytes == 0)
    {
        LOG_TRACE(logger, "Reserved 0 bytes on local disk {}", backQuote(name));
        ++reservation_count;
        return {unreserved_space};
    }

    if (unreserved_space >= bytes)
    {
        LOG_TRACE(
            logger,
            "Reserved {} on local disk {}, having unreserved {}.",
            ReadableSize(bytes),
            backQuote(name),
            ReadableSize(unreserved_space));
        ++reservation_count;
        reserved_bytes += bytes;
        return {unreserved_space - bytes};
    }
    else
    {
        LOG_TRACE(logger, "Could not reserve {} on local disk {}. Not enough unreserved space", ReadableSize(bytes), backQuote(name));
    }

    return {};
}

static UInt64 getTotalSpaceByName(const String & name, const String & disk_path, UInt64 keep_free_space_bytes)
{
    struct statvfs fs;
    if (name == "default") /// for default disk we get space from path/data/
        fs = getStatVFS((fs::path(disk_path) / "data/").string());
    else
        fs = getStatVFS(disk_path);
    UInt64 total_size = fs.f_blocks * fs.f_frsize;
    if (total_size < keep_free_space_bytes)
        return 0;
    return total_size - keep_free_space_bytes;
}

UInt64 DiskCubeFS::getTotalSpace() const
{
    if (broken || readonly)
        return 0;
    return getTotalSpaceByName(name, disk_path, keep_free_space_bytes);
}

UInt64 DiskCubeFS::getAvailableSpace() const
{
    if (broken || readonly)
        return 0;
    /// we use f_bavail, because part of b_free space is
    /// available for superuser only and for system purposes
    struct statvfs fs;
    if (name == "default") /// for default disk we get space from path/data/
        fs = getStatVFS((fs::path(disk_path) / "data/").string());
    else
        fs = getStatVFS(disk_path);
    UInt64 total_size = fs.f_bavail * fs.f_frsize;
    if (total_size < keep_free_space_bytes)
        return 0;
    return total_size - keep_free_space_bytes;
}

UInt64 DiskCubeFS::getUnreservedSpace() const
{
    std::lock_guard lock(DiskCubeFS::reservation_mutex);
    auto available_space = getAvailableSpace();
    available_space -= std::min(available_space, reserved_bytes);
    return available_space;
}

bool DiskCubeFS::exists(const String & path) const
{
    return fs::exists(fs::path(disk_path) / path);
}

bool DiskCubeFS::isFile(const String & path) const
{
    return fs::is_regular_file(fs::path(disk_path) / path);
}

bool DiskCubeFS::isDirectory(const String & path) const
{
    return fs::is_directory(fs::path(disk_path) / path);
}

size_t DiskCubeFS::getFileSize(const String & path) const
{
    return fs::file_size(fs::path(disk_path) / path);
}

void DiskCubeFS::createDirectory(const String & path)
{
    fs::create_directory(fs::path(disk_path) / path);
}

void DiskCubeFS::createDirectories(const String & path)
{
    fs::create_directories(fs::path(disk_path) / path);
}

void DiskCubeFS::clearDirectory(const String & path)
{
    for (const auto & entry : fs::directory_iterator(fs::path(disk_path) / path))
        fs::remove(entry.path());
}

void DiskCubeFS::moveDirectory(const String & from_path, const String & to_path)
{
    fs::rename(fs::path(disk_path) / from_path, fs::path(disk_path) / to_path);
}

DirectoryIteratorPtr DiskCubeFS::iterateDirectory(const String & path) const
{
    fs::path meta_path = fs::path(disk_path) / path;
    if (!broken && fs::exists(meta_path) && fs::is_directory(meta_path))
        return std::make_unique<DiskCubeFSDirectoryIterator>(disk_path, path);
    else
        return std::make_unique<DiskCubeFSDirectoryIterator>();
}

void DiskCubeFS::moveFile(const String & from_path, const String & to_path)
{
    renameNoReplace(fs::path(disk_path) / from_path, fs::path(disk_path) / to_path);
}

void DiskCubeFS::replaceFile(const String & from_path, const String & to_path)
{
    fs::path from_file = fs::path(disk_path) / from_path;
    fs::path to_file = fs::path(disk_path) / to_path;
    fs::create_directories(to_file.parent_path());
    fs::rename(from_file, to_file);
}

std::unique_ptr<ReadBufferFromFileBase> DiskCubeFS::readFile(const String & path, const ReadSettings & settings, std::optional<size_t> read_hint, std::optional<size_t> file_size) const
{
    if (!file_size.has_value())
        file_size = fileSizeSafe(fs::path(disk_path) / path);
    return createReadBufferFromFileBase(fs::path(disk_path) / path, settings, read_hint, file_size);
}

std::unique_ptr<WriteBufferFromFileBase>
DiskCubeFS::writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings &)
{
    int flags = (mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    return std::make_unique<WriteBufferFromFile>(fs::path(disk_path) / path, buf_size, flags);
}

void DiskCubeFS::removeFile(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != unlink(fs_path.c_str()))
        throwFromErrnoWithPath("Cannot unlink file " + fs_path.string(), fs_path, ErrorCodes::CANNOT_UNLINK);
}

void DiskCubeFS::removeFileIfExists(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != unlink(fs_path.c_str()) && errno != ENOENT)
        throwFromErrnoWithPath("Cannot unlink file " + fs_path.string(), fs_path, ErrorCodes::CANNOT_UNLINK);
}

void DiskCubeFS::removeDirectory(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != rmdir(fs_path.c_str()))
        throwFromErrnoWithPath("Cannot rmdir " + fs_path.string(), fs_path, ErrorCodes::CANNOT_RMDIR);
}

void DiskCubeFS::removeRecursive(const String & path)
{
    fs::remove_all(fs::path(disk_path) / path);
}

void DiskCubeFS::listFiles(const String & path, std::vector<String> & file_names) const
{
    file_names.clear();
    String dst_path = fs::path(disk_path) / path;
    if (fs::exists(dst_path) && fs::is_directory(dst_path))
    {
        for (const auto & entry : fs::directory_iterator(dst_path))
            file_names.emplace_back(entry.path().filename());
    }

}

void DiskCubeFS::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    FS::setModificationTime(fs::path(disk_path) / path, timestamp.epochTime());
}

Poco::Timestamp DiskCubeFS::getLastModified(const String & path) const
{
    return FS::getModificationTimestamp(fs::path(disk_path) / path);
}

time_t DiskCubeFS::getLastChanged(const String & path) const
{
    return FS::getChangeTime(fs::path(disk_path) / path);
}

void DiskCubeFS::createHardLink(const String & src_path, const String & dst_path)
{
    DB::createHardLink(fs::path(disk_path) / src_path, fs::path(disk_path) / dst_path);
}

void DiskCubeFS::truncateFile(const String & path, size_t size)
{
    int res = truncate((fs::path(disk_path) / path).string().data(), size);
    if (-1 == res)
        throwFromErrnoWithPath("Cannot truncate file " + path, path, ErrorCodes::CANNOT_TRUNCATE_FILE);
}

void DiskCubeFS::createFile(const String & path)
{
    FS::createFile(fs::path(disk_path) / path);
}

void DiskCubeFS::setReadOnly(const String & path)
{
    fs::permissions(fs::path(disk_path) / path,
                    fs::perms::owner_write | fs::perms::group_write | fs::perms::others_write,
                    fs::perm_options::remove);
}

bool inline isSameDiskType(const IDisk & one, const IDisk & another)
{
    return typeid(one) == typeid(another);
}

void DiskCubeFS::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    if (isSameDiskType(*this, *to_disk))
    {
        fs::path to = fs::path(to_disk->getPath()) / to_path;
        fs::path from = fs::path(disk_path) / from_path;
        if (from_path.ends_with('/'))
            from = from.parent_path();
        if (fs::is_directory(from))
            to /= from.filename();

        fs::copy(from, to, fs::copy_options::recursive | fs::copy_options::overwrite_existing); /// Use more optimal way.
    }
    else
        copyThroughBuffers(from_path, to_disk, to_path, /* copy_root_dir */ true); /// Base implementation.
}

void DiskCubeFS::copyDirectoryContent(const String & from_dir, const std::shared_ptr<IDisk> & to_disk, const String & to_dir)
{
    if (isSameDiskType(*this, *to_disk))
        fs::copy(from_dir, to_dir, fs::copy_options::recursive | fs::copy_options::overwrite_existing); /// Use more optimal way.
    else
        copyThroughBuffers(from_dir, to_disk, to_dir, /* copy_root_dir */ false); /// Base implementation.
}

SyncGuardPtr DiskCubeFS::getDirectorySyncGuard(const String & path) const
{
    return std::make_unique<LocalDirectorySyncGuard>(fs::path(disk_path) / path);
}


void DiskCubeFS::applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap &)
{
    String new_disk_path;
    UInt64 new_keep_free_space_bytes;

    loadDiskCubeFSConfig(name, config, config_prefix, context, new_disk_path, new_keep_free_space_bytes);

    if (disk_path != new_disk_path)
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Disk path can't be updated from config {}", name);

    if (keep_free_space_bytes != new_keep_free_space_bytes)
        keep_free_space_bytes = new_keep_free_space_bytes;
}

DiskCubeFS::DiskCubeFS(const String & name_, const String & path_, UInt64 keep_free_space_bytes_)
    : IDisk(name_)
    , disk_path(path_)
    , keep_free_space_bytes(keep_free_space_bytes_)
    , logger(&Poco::Logger::get("DiskCubeFS"))
{
    data_source_description.type = DataSourceType::CubeFS;

    if (auto block_device_id = tryGetBlockDeviceId(disk_path); block_device_id.has_value())
        data_source_description.description = *block_device_id;
    else
        data_source_description.description = disk_path;
    data_source_description.is_encrypted = false;
    data_source_description.is_cached = false;
}

DiskCubeFS::DiskCubeFS(
    const String & name_, const String & path_, UInt64 keep_free_space_bytes_, ContextPtr context, UInt64 local_disk_check_period_ms)
    : DiskCubeFS(name_, path_, keep_free_space_bytes_)
{
    if (local_disk_check_period_ms > 0)
        disk_checker = std::make_unique<DiskCubeFSCheckThread>(this, context, local_disk_check_period_ms);
}


DiskCubeFS::~DiskCubeFS() {

}

DataSourceDescription DiskCubeFS::getDataSourceDescription() const
{
    return data_source_description;
}

void DiskCubeFS::shutdown()
{
    if (disk_checker)
        disk_checker->shutdown();
}

std::optional<UInt32> DiskCubeFS::readDiskCheckerMagicNumber() const noexcept
try
{
    ReadSettings read_settings;
    /// Proper disk read checking requires direct io
    read_settings.direct_io_threshold = 1;
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
    tryLogCurrentException(logger, fmt::format("Cannot read correct disk check magic number from from {}{}", disk_path, disk_checker_path));
    return {};
}

bool DiskCubeFS::canRead() const noexcept
try
{
    if (FS::canRead(fs::path(disk_path) / disk_checker_path))
    {
        auto magic_number = readDiskCheckerMagicNumber();
        if (magic_number && *magic_number == disk_checker_magic_number)
            return true;
    }
    return false;
}
catch (...)
{
    LOG_WARNING(logger, "Cannot achieve read over the disk directory: {}", disk_path);
    return false;
}

struct DiskWriteCheckData
{
    constexpr static size_t PAGE_SIZE_IN_BYTES = 4096;
    char data[PAGE_SIZE_IN_BYTES]{};
    DiskWriteCheckData()
    {
        static const char * magic_string = "ClickHouse disk local write check";
        static size_t magic_string_len = strlen(magic_string);
        memcpy(data, magic_string, magic_string_len);
        memcpy(data + PAGE_SIZE_IN_BYTES - magic_string_len, magic_string, magic_string_len);
    }
};

bool DiskCubeFS::canWrite() const noexcept
try
{
    static DiskWriteCheckData data;
    String tmp_template = fs::path(disk_path) / "";
    {
        auto buf = WriteBufferFromTemporaryFile::create(tmp_template);
        buf->write(data.data, data.PAGE_SIZE_IN_BYTES);
        buf->sync();
    }
    return true;
}
catch (...)
{
    LOG_WARNING(logger, "Cannot achieve write over the disk directory: {}", disk_path);
    return false;
}

DiskObjectStoragePtr DiskCubeFS::createDiskObjectStorage()
{
    auto object_storage = std::make_shared<LocalObjectStorage>();
    auto metadata_storage = std::make_shared<FakeMetadataStorageFromDisk>(
        /* metadata_storage */std::static_pointer_cast<DiskCubeFS>(shared_from_this()),
        object_storage,
        /* object_storage_root_path */getPath());

    return std::make_shared<DiskObjectStorage>(
        getName(),
        disk_path,
        "Local",
        metadata_storage,
        object_storage,
        false,
        /* threadpool_size */16
    );
}

void DiskCubeFS::checkAccessImpl(const String & path)
{
    try
    {
        fs::create_directories(disk_path);
        if (!FS::canWrite(disk_path))
        {
            LOG_ERROR(logger, "Cannot write to the root directory of disk {} ({}).", name, disk_path);
            readonly = true;
            return;
        }
    }
    catch (...)
    {
        LOG_ERROR(logger, "Cannot create the root directory of disk {} ({}).", name, disk_path);
        readonly = true;
        return;
    }

    IDisk::checkAccessImpl(path);
}

void DiskCubeFS::setup()
{
    try
    {
        if (!FS::canRead(disk_path))
            throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "There is no read access to disk {} ({}).", name, disk_path);
    }
    catch (...)
    {
        LOG_ERROR(logger, "Cannot gain read access of the disk directory: {}", disk_path);
        throw;
    }

    /// If disk checker is disabled, just assume RW by default.
    if (!disk_checker)
        return;

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
    {
        try
        {
            pcg32_fast rng(randomSeed());
            UInt32 magic_number = rng();
            {
                auto buf = writeFile(disk_checker_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
                writeIntBinary(magic_number, *buf);
            }
            disk_checker_magic_number = magic_number;
        }
        catch (...)
        {
            LOG_WARNING(
                logger,
                "Cannot create/write to {0}. Disk {1} is either readonly or broken. Without setting up disk checker file, DiskCubeFSCheckThread "
                "will not be started. Disk is assumed to be RW. Try manually fix the disk and do `SYSTEM RESTART DISK {1}`",
                disk_checker_path,
                name);
            disk_checker_can_check_read = false;
            return;
        }
    }

    if (disk_checker_magic_number == -1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "disk_checker_magic_number is not initialized. It's a bug");
}

void DiskCubeFS::startupImpl(ContextPtr)
{
    broken = false;
    disk_checker_magic_number = -1;
    disk_checker_can_check_read = true;

    try
    {
        setup();
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

struct stat DiskCubeFS::stat(const String & path) const
{
    struct stat st;
    auto full_path = fs::path(disk_path) / path;
    if (::stat(full_path.string().c_str(), &st) == 0)
        return st;
    DB::throwFromErrnoWithPath("Cannot stat file: " + path, path, DB::ErrorCodes::CANNOT_STAT);
}

void DiskCubeFS::chmod(const String & path, mode_t mode)
{
    auto full_path = fs::path(disk_path) / path;
    if (::chmod(full_path.string().c_str(), mode) == 0)
        return;
    DB::throwFromErrnoWithPath("Cannot chmod file: " + path, path, DB::ErrorCodes::PATH_ACCESS_DENIED);
}

MetadataStoragePtr DiskCubeFS::getMetadataStorage()
{
    auto object_storage = std::make_shared<LocalObjectStorage>();
    return std::make_shared<FakeMetadataStorageFromDisk>(
        std::static_pointer_cast<IDisk>(shared_from_this()), object_storage, getPath());
}

}

