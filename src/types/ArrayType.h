#pragma once

#include <memory>

namespace spark::sql::types
{
class DataType;

struct ArrayType
{
    std::shared_ptr<DataType> element_type;
    bool contains_null;
};
} // namespace spark::sql::types
