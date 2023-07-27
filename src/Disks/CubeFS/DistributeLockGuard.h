#pragma once
#include <string>
#include <memory>
#include <base/types.h>
#include <Common/ZooKeeper/ZooKeeper.h>


namespace DB
{

class DistributeLockGuard
{
public:
    DistributeLockGuard(const zkutil::ZooKeeperPtr & zk, const String &zk_path);
    ~DistributeLockGuard();
    bool tryLock(const String & value = "", const String & owner ="");
    void unlock();

private:
    /// add distribute lock for cubefs
    bool tryCreateDistributeLock(const String & value, const String & owner) const;
    bool releaseDistributeLock() const;
    String getLockOwner(String node_value) const;

private:
    zkutil::ZooKeeperPtr zookeeper;

    String lock_path;
    String lock_message;
    bool locked;
    Poco::Logger * log;
    static const String owner_tag;

};

using DistributeLockGuardPtr = std::shared_ptr<DistributeLockGuard>;

}
