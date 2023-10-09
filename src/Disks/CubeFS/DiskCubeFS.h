#pragma once

#include <libcfs.h>
#include <Disks/IDisk.h>

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

class DiskCubeFS final : public IDisk
{
public:
    friend class DiskCubeFSReservation;
    using SettingsPtr = std::unique_ptr<DiskCubeFSSettings>;

    DiskCubeFS(const String & name_, const String & path_, SettingsPtr settings_);
    DiskCubeFS(const String & name_, const String & path_, ContextPtr context, SettingsPtr settings_);
    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & path, size_t buf_size, WriteMode mode) override;
    bool exists(const String & path) const override;
    const String & getName() const override { return name; }
    const String & getPath() const override { return disk_path; }
    ReservationPtr reserve(UInt64 bytes) override;
    UInt64 getTotalSpace() const override;
    UInt64 getUnreservedSpace() const override;
    bool isFile(const String & path) const override;
    bool isDirectory(const String & path) const override;
    size_t getFileSize(const String & path) const override;
    void createDirectory(const String & path) override;
    void createDirectories(const String & path) override;
    /*删除文件夹下的所有文件
    diskLocal类的这个方法没有加锁,通过遍历使用fs::remove方法来删除
    因此空的子文件夹可以删除，子文件夹下有文件会报错
    */
    void clearDirectory(const String & path) override;
    //遵循fs::rename语义，目的端上级目录不错在会报错
    void moveDirectory(const String & from_path, const String & to_path) override;
    void moveFile(const String & from_path, const String & to_path) override; //相比replaceFile,不会覆盖目的端同名文件
    void replaceFile(const String & from_path, const String & to_path) override;

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;
    //父级目录不存在会报错
    void createFile(const String & path) override;

    void listFiles(const String & path, std::vector<String> & file_names) override;
    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;
    Poco::Timestamp getLastModified(const String & path) override;
    time_t getLastChanged(const String & path) const override;
    void setReadOnly(const String & path) override;
    void createHardLink(const String & src_path, const String & dst_path) override;
    DiskType getType() const override { return DiskType::CubeFS; }
    bool isRemote() const override { return true; }
    bool supportZeroCopyReplication() const override { return false; }
    void removeRecursive(const String & path) override;
    UInt64 getAvailableSpace() const override;

private:
    //std::optional<UInt32> readDiskCheckerMagicNumber() const noexcept;
    cfs_stat_info getFileAttributes(const String & relative_path) const;
    bool canRead(const std::string & path);
    std::optional<size_t> fileSizeSafe(const fs::path & path);
    bool tryReserve(UInt64 bytes);
    const String name;
    const String disk_path;
    SettingsPtr settings;
    Poco::Logger * logger;
    static std::mutex reservation_mutex;
    std::atomic<UInt64> keep_free_space_bytes;
    std::atomic<bool> broken{false};
    std::atomic<bool> readonly{false};
    UInt64 reserved_bytes = 0;
    //const String disk_checker_path = ".disk_checker_file";
    UInt64 reservation_count = 0;
};
}
