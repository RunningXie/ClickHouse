#include <libcfs.h>
#include "DiskCubeFS.h"
#include "Disks/DiskFactory.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

int64_t getClientId()
{
    int64_t id = cfs_new_client();
    if (id <= 0)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to create client");
    }
    return id;
}

std::unique_ptr<DiskS3Settings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "volumn name: {}", config.getString(config_prefix + ".vol_name"));
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "master address: {}", config.getString(config_prefix + ".master_addr"));
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "log directory: {}", config.getString(config_prefix + ".log_dir"));
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "log level: {}", config.getString(config_prefix + ".log_level"));
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "access key: {}", config.getString(config_prefix + ".access_key"));
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "secret key: {}", config.getString(config_prefix + ".secret_key"));
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "push address: {}", config.getString(config_prefix + ".push_addr"));

    return std::make_unique<DiskS3Settings>(
        getClientId(),
        config.getString(config_prefix + ".vol_name"),
        config.getString(config_prefix + ".master_addr"),
        config.getString(config_prefix + ".log_dir"),
        config.getString(config_prefix + ".log_level", "debug"),
        config.getString(config_prefix + ".access_key"),
        config.getString(config_prefix + ".secret_key"),
        config.getString(config_prefix + ".push_addr"));
}

void registerDiskCubeFS(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      const DisksMap & /*map*/) -> DiskPtr
    {
        std::unique_ptr<DiskS3Settings> settings = getSettings(config, config_prefix);
        // 设置客户端信息
        if (cfs_set_client(id, strdup("volName"), settings->vol_name.c_str()) != 0)
        {
            cfs_close_client(id);
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set client info");
        }
        if (cfs_set_client(id, strdup("masterAddr"), settings->master_addr.c_str()) != 0)
        {
            cfs_close_client(id);
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set client info");
        }
        if (cfs_set_client(id, strdup("logDir"), settings->log_dir.c_str()) != 0)
        {
            cfs_close_client(id);
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set client info");
        }
        if (cfs_set_client(id, strdup("logLevel"), settings->log_level.c_str()) != 0)
        {
            cfs_close_client(id);
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set client info");
        }
        if (cfs_set_client(id, strdup("accessKey"), settings->access_key.c_str()) != 0)
        {
            cfs_close_client(id);
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set client info");
        }
        if (cfs_set_client(id, strdup("secretKey"), settings->secret_key.c_str()) != 0)
        {
            cfs_close_client(id);
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set client info");
        }
        if (cfs_set_client(id, strdup("pushAddr"), settings->push_addr.c_str()) != 0)
        {
            cfs_close_client(id);
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set client info");
        }

        std::shared_ptr<IDisk> cubeFSdisk
            = std::make_shared<DiskCubeFS>(name, config.getString(config_prefix + ".path", ""), context, settings);
        cubeFSDisk->startup();
    };
    factory.registerDiskType("cubefs", creator);
}
}
