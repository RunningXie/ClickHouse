#pragma once
#include <string>
#include <memory>

#include <Common/ZooKeeper/ZooKeeper.h>


namespace DB
{

class DistributeLockGuard
{
public:
    DistributeLockGuard(const zkutil::ZooKeeperPtr & zk, const String &zk_path);
    ~DistributeLockGuard();
    bool tryLock(const String & value = "");
    void unlock();

private:
    /// add distribute lock for cubefs
    bool tryCreateDistributeLock(const String & value) const;
    bool releaseDistributeLock() const;

private:
    zkutil::ZooKeeperPtr zookeeper;

    std::string lock_path;
    std::string lock_message;
    bool locked;
    Poco::Logger * log;

};

using DistributeLockGuardPtr = std::shared_ptr<DistributeLockGuard>;

}
