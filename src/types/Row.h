#pragma once

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <typeinfo>
#include <variant>
#include <vector>

#include <arrow/array.h>

#include "ml/linalg/vectors/dense_vector.h"
#include "ml/linalg/vectors/sparse_vector.h"

namespace spark::sql::types
{
struct Row;
struct ArrayData;
struct MapData;

/**
 * @brief A variant representing a single value in a Row.
 * Includes primitives, decimals, and recursive complex types.
 */
using ColumnValue = std::variant<std::monostate,             // Null
                                 bool,                       // Boolean
                                 int8_t,                     // Byte
                                 int16_t,                    // Short
                                 int32_t,                    // Integer / Date
                                 int64_t,                    // Long / Timestamp
                                 float,                      // Float
                                 double,                     // Double
                                 std::string,                // String
                                 std::vector<uint8_t>,       // Binary
                                 std::shared_ptr<Row>,       // Struct (Nested)
                                 std::shared_ptr<ArrayData>, // Array
                                 std::shared_ptr<MapData>    // Map
                                 >;

struct ArrayData
{
    std::vector<ColumnValue> elements;
};

struct MapData
{
    std::vector<ColumnValue> keys;
    std::vector<ColumnValue> values;
};

struct Row
{
    std::vector<std::string> column_names;
    std::vector<ColumnValue> values;

    /**
     * @brief Access value by column index: row[0]
     */
    const ColumnValue& operator[](size_t index) const
    {
        return values.at(index);
    }

    /**
     * @brief Access value by column name: row["col_name"]
     */
    const ColumnValue& operator[](const std::string& name) const
    {
        return values.at(col_index(name));
    }

    /**
     * @brief Strict access. This fails if the type doesn't match exactly.
     */
    template <typename T> T get(const std::string& name) const
    {
        const auto& val = values.at(col_index(name));

        return std::visit(
            [](auto&& arg) -> T
            {
                using ArgType = std::decay_t<decltype(arg)>;

                // Exact type match
                if constexpr (std::is_same_v<T, ArgType>)
                {
                    return arg;
                }
                // Null -> "null" string (only when T is string)
                else if constexpr (std::is_same_v<T, std::string> &&
                                   std::is_same_v<ArgType, std::monostate>)
                {
                    // Both conditions must be true for this branch to be
                    // instantiated, so returning a string literal is always
                    // valid here, but we cast to T to keep clang happy
                    // when it typechecks non-taken branches.
                    if constexpr (std::is_same_v<T, std::string>)
                        return T{"null"};
                    else
                        throw std::runtime_error("unreachable");
                }
                // Numeric widening
                else if constexpr (std::is_arithmetic_v<T> && std::is_arithmetic_v<ArgType>)
                {
                    return static_cast<T>(arg);
                }
                // Null -> empty vector<string>
                else if constexpr (std::is_same_v<T, std::vector<std::string>> &&
                                   std::is_same_v<ArgType, std::monostate>)
                {
                    return T{};
                }
                // ArrayData -> vector<string>
                else if constexpr (std::is_same_v<T, std::vector<std::string>> &&
                                   std::is_same_v<ArgType, std::shared_ptr<ArrayData>>)
                {
                    if constexpr (std::is_same_v<T, std::vector<std::string>>)
                    {
                        std::vector<std::string> string_list;
                        for (size_t i = 0; i < arg->elements.size(); i++)
                            string_list.push_back(std::get<std::string>(arg->elements[i]));
                        return string_list;
                    }
                    else
                        throw std::runtime_error("unreachable");
                }
                // Row -> SparseVector
                else if constexpr (std::is_same_v<T, SparseVector> &&
                                   std::is_same_v<ArgType, std::shared_ptr<Row>>)
                {
                    if constexpr (std::is_same_v<T, SparseVector>)
                    {
                        SparseVector sparse_vector;

                        auto type_field = std::find_if(arg->column_names.begin(), arg->column_names.end(), [](const std::string& col_name) {
                            return col_name == "type";
                        });

                        if (type_field == arg->column_names.end() || std::get<int8_t>(arg->values.at(arg->col_index("type"))) != 0)
                            return sparse_vector;

                        auto size = std::get<int32_t>(arg->values.at(arg->col_index("size")));
                        auto indice_array_data = std::get<std::shared_ptr<ArrayData>>(arg->values.at(arg->col_index("indices")));
                        auto values_array_data = std::get<std::shared_ptr<ArrayData>>(arg->values.at(arg->col_index("values")));

                        std::vector<int> indices;
                        std::vector<double> values;

                        indices.reserve(indice_array_data->elements.size());
                        values.reserve(values_array_data->elements.size());

                        for (int i = 0; i < indice_array_data->elements.size(); i++)
                        {
                            indices.push_back(std::get<int32_t>(indice_array_data->elements[i]));
                        }

                        for (int i = 0; i < values_array_data->elements.size(); i++)
                        {
                            values.push_back(std::get<double>(values_array_data->elements[i]));
                        }

                        return SparseVector{size, indices, values};
                    }
                    else
                        throw std::runtime_error("unreachable");
                }
                else
                {
                    throw std::runtime_error("Row::get Type Mismatch: requested "
                                             "type does not match variant "
                                             "state: " +
                                             std::string(typeid(ArgType).name()));
                }
            },
            val);
    }

    /**
     * @brief Widening Integer Access.
     * This retrieves any integral type (int8..int64) as int64_t.
     */
    int64_t get_long(const std::string& name) const
    {
        const auto& val = (*this)[name];
        if (std::holds_alternative<std::monostate>(val))
        {
            throw std::runtime_error("Column " + name + " is null");
        }
        return std::visit(
            [](auto&& arg) -> int64_t
            {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>)
                {
                    return static_cast<int64_t>(arg);
                }
                throw std::runtime_error("Column is not a numeric integral type");
            },
            val);
    }

    /**
     * @brief Widening Floating Point Access.
     * This retrieves any numeric type as a double.
     */
    double get_double(const std::string& name) const
    {
        return std::visit(
            [](auto&& arg) -> double
            {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>)
                {
                    return static_cast<double>(arg);
                }
                throw std::runtime_error("Column is not a numeric type");
            },
            (*this)[name]);
    }

    auto begin() const
    {
        return values.begin();
    }
    auto end() const
    {
        return values.end();
    }
    size_t size() const
    {
        return values.size();
    }

    int col_index(const std::string& name) const
    {
        auto it = std::find(column_names.begin(), column_names.end(), name);
        if (it == column_names.end())
            throw std::runtime_error("Column not found: " + name);
        return static_cast<int>(std::distance(column_names.begin(), it));
    }

    friend std::ostream& operator<<(std::ostream& os, const Row& row);
};

std::ostream& operator<<(std::ostream& os, const Row& row);

/**
 * @brief Converts an Arrow Array value at a specific row into a Spark
 * `ColumnValue`. This acts as a bridge between the Arrow transport layer and
 * the respective C++ Row model.
 *
 * See: `Row` implementation.
 */
ColumnValue arrayValueToVariant(const std::shared_ptr<arrow::Array>& array, int64_t row);

} // namespace spark::sql::types

