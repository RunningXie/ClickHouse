#pragma once

#include <base/logger_useful.h>
#include <Disks/DiskLocal.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB
{
class DiskCubeFSCheckThread;
class DiskCubeFS : public DiskLocal
{
public:
    friend class DiskCubeFSCheckThread;
    friend class DiskCubeFSReservation;

    DiskCubeFS(const String & name_, const String & path_, UInt64 keep_free_space_bytes_);
    DiskCubeFS(
        const String & name_,
        const String & path_,
        UInt64 keep_free_space_bytes_,
        ContextPtr context,
        UInt64 local_disk_check_period_ms);
    virtual ~DiskCubeFS() override;

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap &) override;
    bool supportDataSharing() const override { return true; }
    bool isRemote() const override { return false; }
    bool supportZeroCopyReplication() const override { return false; }

};

extern void loadDiskCubeFSConfig(const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      String & path,
                      UInt64 & keep_free_space_bytes);

using DiskCubeFSPtr = std::shared_ptr<DiskCubeFS>;

}



