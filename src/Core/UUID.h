#pragma once

#include <Core/Types.h>


namespace DB
{

namespace UUIDHelpers
{
    /// Generate random UUID.
    UUID generateV4();
    UUID generateV4ByDatabaseAndTable(const std::string& databaseName, const std::string& tableName);

    const UUID Nil{};
}

}
