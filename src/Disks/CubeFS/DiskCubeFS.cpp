#include <Disks/CubeFS/DiskCubeFS.h>
#include <Disks/CubeFS/DiskCubeFSCheckThread.h>
#include <Interpreters/Context.h>
#include <Common/createHardLink.h>
#include <Disks/DiskFactory.h>

#include <Disks/LocalDirectorySyncGuard.h>
#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>
#include <Disks/IO/createReadBufferFromFileBase.h>

#include <fstream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <Disks/DiskFactory.h>
#include <Common/randomSeed.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <IO/WriteHelpers.h>
#include <base/logger_useful.h>

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

class DiskCubeFSDirectoryIterator final : public IDiskDirectoryIterator
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


DiskDirectoryIteratorPtr DiskCubeFS::iterateDirectory(const String & path)
{
    fs::path meta_path = fs::path(disk_path) / path;
    if (!broken && fs::exists(meta_path) && fs::is_directory(meta_path))
        return std::make_unique<DiskCubeFSDirectoryIterator>(disk_path, path);
    else
        return std::make_unique<DiskCubeFSDirectoryIterator>();
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
    : DiskLocal(name_, path_, keep_free_space_bytes_)
{
    logger = &Poco::Logger::get("DiskCubeFS");
}

DiskCubeFS::DiskCubeFS(
    const String & name_, const String & path_, UInt64 keep_free_space_bytes_, [[maybe_unused]] ContextPtr context, [[maybe_unused]] UInt64 local_disk_check_period_ms)
    : DiskCubeFS(name_, path_, keep_free_space_bytes_)
{
    // if (local_disk_check_period_ms > 0)
    //    disk_checker = std::make_unique<DiskCubeFSCheckThread>(this, context, local_disk_check_period_ms);
}


DiskCubeFS::~DiskCubeFS() {

}

}

