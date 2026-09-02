#pragma once

#include <memory>

namespace spark::sql::types
{
class DataType;

struct MapType
{
    std::shared_ptr<DataType> key_type;
    std::shared_ptr<DataType> value_type;
    bool value_contains_null;
};
} // namespace spark::sql::types

