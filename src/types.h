#pragma once

#include <string>
#include <vector>
#include <memory>
#include <variant>
#include <optional>
#include <cstdint>

#include <arrow/array.h>

namespace spark::connect
{
    class DataType;
}

namespace spark::sql::types
{
    /**
     * @brief Basic Spark Types
     */
    struct NullType
    {
    };
    struct BooleanType
    {
    };
    struct ByteType
    {
    };
    struct ShortType
    {
    };
    struct IntegerType
    {
    };
    struct LongType
    {
    };
    struct FloatType
    {
    };
    struct DoubleType
    {
    };
    struct StringType
    {
    };
    struct BinaryType
    {
    };
    struct DateType
    {
    };
    struct TimestampType
    {
    };
    struct TimestampNtzType
    {
    };

    /**
     * @brief Parameterized Types
     */
    struct DecimalType
    {
        int32_t precision = 10;
        int32_t scale = 0;
        DecimalType() = default;
        DecimalType(int32_t p, int32_t s) : precision(p), scale(s) {}
    };

    struct CharType
    {
        int32_t length;
    };
    struct VarCharType
    {
        int32_t length;
    };

    class DataType;
    struct StructField;

    /**
     * @brief Complex Types
     */
    struct ArrayType
    {
        std::shared_ptr<DataType> element_type;
        bool contains_null;
    };

    struct MapType
    {
        std::shared_ptr<DataType> key_type;
        std::shared_ptr<DataType> value_type;
        bool value_contains_null;
    };

    struct StructType
    {
        std::vector<StructField> fields;
        std::string json() const;
        void print_tree(std::ostream &os) const;
    };

    /**
     * @brief Variant and Wrapper
     */
    using DataTypeVariant = std::variant<
        NullType, BooleanType, ByteType, ShortType, IntegerType, LongType,
        FloatType, DoubleType, StringType, BinaryType, DateType,
        TimestampType, TimestampNtzType, DecimalType, CharType, VarCharType,
        ArrayType, MapType, StructType>;

    class DataType
    {
    public:
        DataTypeVariant kind;

        DataType(DataTypeVariant k) : kind(std::move(k)) {}

        /**
         * @brief Returns the JSON representation of the type, compatible with Spark's StructType.json().
         */
        std::string json() const;

        /**
         * @brief Returns a simple string name for the type (e.g., "integer", "struct").
         */
        std::string type_name() const;

        /**
         * @brief Factory method to create a DataType from a Spark Connect Protobuf message.
         */
        static DataType from_proto(const spark::connect::DataType &proto);
    };

    struct StructField
    {
        std::string name;
        DataType data_type;
        bool nullable = true;
        std::optional<std::string> metadata;
    };

    struct Row;
    struct ArrayData;
    struct MapData;

    /**
     * @brief A variant representing a single value in a Row.
     * Includes primitives, decimals, and recursive complex types.
     */
    using ColumnValue = std::variant<
        std::monostate,             // Null
        bool,                       // Boolean
        int8_t,                     // Byte
        int16_t,                    // Short
        int32_t,                    // Integer / Date (Days since epoch)
        int64_t,                    // Long / Timestamp (Micros since epoch)
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
        const ColumnValue &operator[](size_t index) const
        {
            return values.at(index);
        }

        /**
         * @brief Access value by column name: row["col_name"]
         */
        const ColumnValue &operator[](const std::string &name) const
        {
            return values.at(col_index(name));
        }

        /**
         * @brief Get value by name with explicit type: row.get<int32_t>("id")
         */
        template <typename T>
        const T &get(const std::string &name) const
        {
            return std::get<T>(values.at(col_index(name)));
        }

        /**
         * @brief Get value by index with explicit type: row.get<int32_t>(0)
         */
        template <typename T>
        const T &get(size_t index) const
        {
            return std::get<T>(values.at(index));
        }

        // ---------------------------------
        // Iteration Support
        //
        // In oder to support iterating through the result,
        // the following logic with
        // ---------------------------------

        auto begin() const { return values.begin(); }
        auto end() const { return values.end(); }

        size_t size() const { return values.size(); }

        // Helper to find index by name
        int col_index(const std::string &name) const
        {
            auto it = std::find(column_names.begin(), column_names.end(), name);
            if (it == column_names.end())
                throw std::runtime_error("Column not found: " + name);
            return static_cast<int>(std::distance(column_names.begin(), it));
        }

        friend std::ostream &operator<<(std::ostream &os, const Row &row);
    };

    std::ostream &operator<<(std::ostream &os, const Row &row);

    /**
     * @brief Converts an Arrow Array value at a specific row into a Spark ColumnValue.
     * This is the bridge between the Arrow transport layer and our C++ Row model.
     */
    ColumnValue arrayValueToVariant(const std::shared_ptr<arrow::Array> &array, int64_t row);
}