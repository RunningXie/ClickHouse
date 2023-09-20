#include <libcfs.h>
#include "DiskCubeFS.h"
#include "Disks/DiskFactory.h"
#include "Disks/DiskRestartProxy.h"

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

std::unique_ptr<DiskCubeFSSettings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "volumn name: {}", config.getString(config_prefix + ".vol_name"));
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "master address: {}", config.getString(config_prefix + ".master_addr"));
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "log directory: {}", config.getString(config_prefix + ".log_dir"));
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "log level: {}", config.getString(config_prefix + ".log_level"));
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "access key: {}", config.getString(config_prefix + ".access_key"));
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "secret key: {}", config.getString(config_prefix + ".secret_key"));
    LOG_DEBUG(&Poco::Logger::get("DiskCubeFS"), "push address: {}", config.getString(config_prefix + ".push_addr"));

    return std::make_unique<DiskCubeFSSettings>(
        getClientId(),
        config.getString(config_prefix + ".vol_name"),
        config.getString(config_prefix + ".master_addr"),
        config.getString(config_prefix + ".log_dir"),
        config.getString(config_prefix + ".log_level", "debug"),
        config.getString(config_prefix + ".access_key"),
        config.getString(config_prefix + ".secret_key"),
        config.getString(config_prefix + ".push_addr"));
}

void setClientInfo(int id, const char * key, char * value)
{
    if (cfs_set_client(id, strdup(key), value) != 0)
    {
        cfs_close_client(id);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set client info");
    }
}

void registerDiskCubeFS(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      const DisksMap & /*map*/) -> DiskPtr
    {
        std::unique_ptr<DiskCubeFSSettings> settings = getSettings(config, config_prefix);
        // 设置客户端信息
        setClientInfo(settings->id, "volName", const_cast<char *>(settings->vol_name.data()));
        setClientInfo(settings->id, "masterAddr", const_cast<char *>(settings->master_addr.data()));
        setClientInfo(settings->id, "logDir", const_cast<char *>(settings->log_dir.data()));
        setClientInfo(settings->id, "logLevel", const_cast<char *>(settings->log_level.data()));
        setClientInfo(settings->id, "accessKey", const_cast<char *>(settings->access_key.data()));
        setClientInfo(settings->id, "secretKey", const_cast<char *>(settings->secret_key.data()));
        setClientInfo(settings->id, "pushAddr", const_cast<char *>(settings->push_addr.data()));

        std::shared_ptr<IDisk> cubeFSdisk
            = std::make_shared<DiskCubeFS>(name, config.getString(config_prefix + ".path", ""), context, settings);
        return std::make_shared<DiskRestartProxy>( cubeFSdisk);
    };
    factory.registerDiskType("cubefs", creator);
}
}
