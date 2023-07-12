#include <Disks/DiskFactory.h>
#include <Disks/CubeFS/DiskCubeFS.h>
#include <Common/Macros.h>
#include <Common/logger_useful.h>

namespace DB
{


void registerDiskCubeFS(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
            const String & name,
            const Poco::Util::AbstractConfiguration & config,
            const String & config_prefix,
            ContextPtr context,
            const DisksMap & map) -> DiskPtr
        {
            String path;
            UInt64 keep_free_space_bytes;
            loadDiskCubeFSConfig(name, config, config_prefix, context, path, keep_free_space_bytes);

            for (const auto & [disk_name, disk_ptr] : map)
                if (path == disk_ptr->getPath())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk {} and disk {} cannot have the same path ({})", name, disk_name, path);

            bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
            DiskPtr disk
                = std::make_shared<DiskCubeFS>(name, path, keep_free_space_bytes, context, config.getUInt("local_disk_check_period_ms", 0));
            disk->startup(context, skip_access_check);
            return disk;
        };

    factory.registerDiskType("cubefs", creator);
}

}
