#pragma once

#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace spark::sql::types
{
class DataType;

struct StructField
{
    std::string name;
    std::shared_ptr<DataType> data_type;
    bool nullable = true;
    std::optional<std::string> metadata;
};

struct StructType
{
    std::vector<StructField> fields;
    std::string json() const;
    void print_tree(std::ostream& os) const;
};
} // namespace spark::sql::types

