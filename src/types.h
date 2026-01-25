#pragma once

#include <string>
#include <vector>
#include <memory>
#include <variant>
#include <optional>
#include <cstdint>

namespace spark::connect
{
    class DataType;
}

namespace spark::sql::types
{
    // -------------------------
    // Basic Spark Types
    // -------------------------
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

    // ----------------------------
    // Parameterized Types
    // ----------------------------
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

    // --------------------------------
    // Complex Types
    // --------------------------------
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
    };

    // --------------------------------
    // Variant and Wrapper
    // --------------------------------
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

}