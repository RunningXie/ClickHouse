#include <Core/UUID.h>
#include <Common/thread_local_rng.h>
#include <random>


namespace DB
{

namespace UUIDHelpers
{
    UUID generateV4()
    {
        UInt128 res{thread_local_rng(), thread_local_rng()};
        res.items[0] = (res.items[0] & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        res.items[1] = (res.items[1] & 0x3fffffffffffffffull) | 0x8000000000000000ull;
        return UUID{ res };
    }
    UUID generateV4ByDatabaseAndTable(const std::string& databaseName, const std::string& tableName)
    {
        std::hash<std::string> hasher;
        uint64_t seed = hasher(databaseName) ^ hasher(tableName);
        std::mt19937_64 gen(seed);
        std::uniform_int_distribution<uint64_t> dist(0, std::numeric_limits<uint64_t>::max());

        UInt128 res{ dist(gen), dist(gen) };
        res.items[0] = (res.items[0] & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        res.items[1] = (res.items[1] & 0x3fffffffffffffffull) | 0x8000000000000000ull;

        return UUID{ res };
    }
}

}
