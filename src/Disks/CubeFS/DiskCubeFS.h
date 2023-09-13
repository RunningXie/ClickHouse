#pragma once
#include <Disks/IDiskRemote.h>

namespace DB
{
struct DiskCubeFSSettings
{
    DiskCubeFSSettings(
        const int64_t id_,
        String vol_name_,
        String master_addr_,
        String log_dir_,
        String log_level_,
        String access_key_,
        String secret_key_,
        String push_addr_);
    const int64_t id;
    String vol_name;
    String master_addr;
    String log_dir;
    String log_level;
    String access_key;
    String secret_key;
    String push_addr;
};

class DiskCubeFS : public IDisk
{
public:
    using SettingsPtr = std::unique_ptr<DiskCubeFSSettings>;
    DiskCubeFS(const String & name_, const String & path_, ContextPtr context_, SettingsPtr settings_);

    std::unique_ptr<ReadBufferFromFileBase>
    readFile(const String & path, const ReadSettings & settings, std::optional<size_t> read_hint, std::optional<size_t> file_size)
        const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & path, size_t buf_size, WriteMode mode) override;
    bool exists(const String & path) const override;
    const String & getName() const override { return name; }
    const String & getPath() const override { return disk_path; }

private:
    const String name;
    const String disk_path;
    const String disk_checker_path = ".disk_checker_file";
    SettingsPtr settings;
    Poco::Logger * logger;
    std::unique_ptr<DiskLocalCheckThread> disk_checker;
    /// Setup disk for healthy check. Returns true if it's read-write, false if read-only.
    /// Throw exception if it's not possible to setup necessary files and directories.
    bool setup();
    bool canRead(const std::string & path);
    std::optional<size_t> fileSizeSafe(const fs::path & path);
}
}
