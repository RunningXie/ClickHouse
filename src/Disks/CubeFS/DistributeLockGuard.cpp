#include <filesystem>
#include <Disks/CubeFS/DistributeLockGuard.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace fs = std::filesystem;

DistributeLockGuard::DistributeLockGuard(const zkutil::ZooKeeperPtr & zk, const String & zk_path)
    : zookeeper(zk), lock_path(zk_path), locked(false), log(&Poco::Logger::get("CubeFS:DistributeLockGuard"))

{
}

DistributeLockGuard::~DistributeLockGuard()
{
    unlock();
}

bool DistributeLockGuard::tryLock(const String & value)
{
    if (locked)
        return true;
    locked = tryCreateDistributeLock(value);
    return locked;
}

void DistributeLockGuard::unlock()
{
    if (locked)
        locked = !releaseDistributeLock();
}

bool DistributeLockGuard::tryCreateDistributeLock(const String & value) const
{
    /// In rare case other replica can remove path between createAncestors and createIfNotExists
    /// So we make up to 5 attempts
    if (!zookeeper)
    {
        LOG_WARNING(log, "Cannot create distribute lock for znode {} without zookeeper", lock_path);
        return false;
    }

    int attempts = 2;
    while (attempts > 0)
    {
        try
        {
            auto code = zookeeper->tryCreate(lock_path, value, zkutil::CreateMode::Ephemeral);
            if (code == Coordination::Error::ZOK)
            {
                LOG_TRACE(log, "Create lock for znode {}", lock_path);
                return true;
            }
            else if (code == Coordination::Error::ZNODEEXISTS)
            {
                LOG_INFO(log, "Cannot create lock for znode {} because lock was locked by others", lock_path);
                return false;
            }
            throw zkutil::KeeperException(code, lock_path);
        }
        catch (const zkutil::KeeperException & e)
        {
            --attempts;
            if (e.code == Coordination::Error::ZBADVERSION)
            {
                continue;
            }

            String parent_path = fs::path(lock_path).parent_path();
            if (e.code == Coordination::Error::ZNONODE && !zookeeper->exists(parent_path))
            {
                /// compatibility for old tables built before upgrading.
                try
                {
                    zookeeper->createAncestors(lock_path);
                    LOG_INFO(log, "Cannot create lock for znode {} because exception {}  occurs from zookeeper"
                                "Maybe table is built before clickhouse upgrading, so create ancestor znode {}",
                                lock_path, e.message(), parent_path);
                }
                catch (const zkutil::KeeperException & e)
                {
                    LOG_WARNING(log, "Cannot create lock for znode {} because exception occurs from zookeeper,excetpion : {}",
                                 parent_path, e.displayText());
                    return false;
                }
            }
            else
            {
                LOG_WARNING(log, "Cannot create lock for znode {} because exception occurs from zookeeper,excetpion : {}",
                             lock_path, e.displayText());
                return false;
            }
        }
    }

    return false;
}

bool DistributeLockGuard::releaseDistributeLock() const
{
    if (!zookeeper)
    {
        LOG_WARNING(log, "Can't Relase distribute lock without zookeeper");
        return true;
    }

    if (zookeeper->expired())
    {
        LOG_WARNING(log, "Lock is lost, because session was expired. Path: {}", lock_path);
        return true;
    }

    int attempts = 2;
    while (attempts > 0)
    {
        try
        {
            Coordination::Stat stat;
            bool existed = zookeeper->exists(lock_path, &stat);
            if (!existed)
            {
                LOG_WARNING(log, "Release lock failed, znode {} is not existed now. maybe lock is released by others because of lock timeout", lock_path);
                return true;
            }

            zookeeper->remove(lock_path);
            LOG_TRACE(log, "Release lock for znode {}", lock_path);
            return true;
        }
        catch (const zkutil::KeeperException & e)
        {
            --attempts;
            if (e.code == Coordination::Error::ZBADVERSION)
            {
                continue;
            }
            else if (e.code == Coordination::Error::ZNONODE)
            {
                LOG_INFO(log, "Cannot Release lock for znode {} because lock was unlocked by others", lock_path);
                return true;
            }
            else
            {
                LOG_WARNING(log, "Cannot release lock for znode {} because exception occurs from zookeeper,excetpion {}",
                        lock_path, e.displayText());
                return false;
            }
        }
    }

    return false;
}

}
