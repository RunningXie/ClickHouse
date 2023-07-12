#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context_fwd.h>

namespace Poco
{
class Logger;
}

namespace DB
{
class DiskCubeFS;

class DiskCubeFSCheckThread : WithContext
{
public:
    friend class DiskCubeFS;

    DiskCubeFSCheckThread(DiskCubeFS * disk_, ContextPtr context_, UInt64 local_disk_check_period_ms);

    void startup();

    void shutdown();

private:
    bool check();
    void run();

    DiskCubeFS * disk;
    size_t check_period_ms;
    Poco::Logger * log;
    std::atomic<bool> need_stop{false};

    BackgroundSchedulePool::TaskHolder task;
    size_t retry{};
};

}
