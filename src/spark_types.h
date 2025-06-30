#ifndef SPARK_CONNECT_CPP_SPARK_TYPES_H
#define SPARK_CONNECT_CPP_SPARK_TYPES_H

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <optional>
#include <stdexcept> // For exceptions

//--------------------------------------------
// Include the full Protobuf definition here
//--------------------------------------------
#include "spark/connect/types.pb.h"

namespace spark
{
    namespace client
    {
        //-------------------------------------------------------------
        // Forward declarations for all concrete SparkDataType types
        //-------------------------------------------------------------
        class SparkDataType;
        class BooleanType;
        class ByteType;
        class ShortType;
        class IntegerType;
        class LongType;
        class FloatType;
        class DoubleType;
        class DecimalType;
        class StringType;
        class CharType;
        class VarCharType;
        class BinaryType;
        class NullType;
        class DateType;
        class TimestampType;
        class TimestampNTZType;
        class CalendarIntervalType;
        class YearMonthIntervalType;
        class DayTimeIntervalType;
        class ArrayType;
        class StructType;
        class StructField; // Note: StructField is a helper, not directly a SparkDataType
        class MapType;
        class VariantType;
        class UDTType;
        class UnparsedType;

        /**
         * @brief Base abstract class for all Spark Data Types.
         * This class now directly holds the spark::connect::DataType Protobuf message.
         */
        class SparkDataType
        {
        public:
            enum class TypeId
            {
                UNKNOWN,
                NULL_TYPE,
                BINARY,
                BOOLEAN,
                BYTE,
                SHORT,
                INTEGER,
                LONG,
                FLOAT,
                DOUBLE,
                DECIMAL,
                STRING,
                CHAR,
                VARCHAR,
                DATE,
                TIMESTAMP,
                TIMESTAMP_NTZ,
                CALENDAR_INTERVAL,
                YEAR_MONTH_INTERVAL,
                DAY_TIME_INTERVAL,
                ARRAY,
                STRUCT,
                MAP,
                VARIANT,
                UDT,
                UNPARSED
            };

            //-------------------------------------------------------------
            // Constructor can directly initialize the proto_
            //-------------------------------------------------------------
            SparkDataType() = default;

            //-------------------------------------------------------------------------
            // Constructor to create from an existing Protobuf message (for FromProto)
            //-------------------------------------------------------------------------
            explicit SparkDataType(const spark::connect::DataType &proto) : proto_(proto) {}
            virtual ~SparkDataType() = default;

            //-------------------------------------------------------------------------
            // Factory method to create a SparkDataType from a Protobuf DataType
            //-------------------------------------------------------------------------
            static std::shared_ptr<SparkDataType> FromProto(const spark::connect::DataType &proto_type);

            //-------------------------------------------------------------------------
            // Get the underlying Protobuf message
            //-------------------------------------------------------------------------
            const spark::connect::DataType &ToProto() const { return proto_; }
            spark::connect::DataType *MutableProto() { return &proto_; } // For internal use by derived classes

            //-------------------------------------------------------------------------
            // Pure virtual method to get the specific TypeId
            //-------------------------------------------------------------------------
            virtual TypeId GetTypeId() const = 0;

            //-------------------------------------------------------------------------
            // Common type_variation_reference accessor (accesses from internal proto_)
            //-------------------------------------------------------------------------
            uint32_t type_variation_reference() const;

            //-------------------------------------------------------------------------
            // Convenience methods to check the type
            //-------------------------------------------------------------------------
            bool isNull() const { return GetTypeId() == TypeId::NULL_TYPE; }
            bool isBinary() const { return GetTypeId() == TypeId::BINARY; }
            bool isBoolean() const { return GetTypeId() == TypeId::BOOLEAN; }
            bool isByte() const { return GetTypeId() == TypeId::BYTE; }
            bool isShort() const { return GetTypeId() == TypeId::SHORT; }
            bool isInteger() const { return GetTypeId() == TypeId::INTEGER; }
            bool isLong() const { return GetTypeId() == TypeId::LONG; }
            bool isFloat() const { return GetTypeId() == TypeId::FLOAT; }
            bool isDouble() const { return GetTypeId() == TypeId::DOUBLE; }
            bool isDecimal() const { return GetTypeId() == TypeId::DECIMAL; }
            bool isString() const { return GetTypeId() == TypeId::STRING; }
            bool isChar() const { return GetTypeId() == TypeId::CHAR; }
            bool isVarChar() const { return GetTypeId() == TypeId::VARCHAR; }
            bool isDate() const { return GetTypeId() == TypeId::DATE; }
            bool isTimestamp() const { return GetTypeId() == TypeId::TIMESTAMP; }
            bool isTimestampNTZ() const { return GetTypeId() == TypeId::TIMESTAMP_NTZ; }
            bool isCalendarInterval() const { return GetTypeId() == TypeId::CALENDAR_INTERVAL; }
            bool isYearMonthInterval() const { return GetTypeId() == TypeId::YEAR_MONTH_INTERVAL; }
            bool isDayTimeInterval() const { return GetTypeId() == TypeId::DAY_TIME_INTERVAL; }
            bool isArray() const { return GetTypeId() == TypeId::ARRAY; }
            bool isStruct() const { return GetTypeId() == TypeId::STRUCT; }
            bool isMap() const { return GetTypeId() == TypeId::MAP; }
            bool isVariant() const { return GetTypeId() == TypeId::VARIANT; }
            bool isUDT() const { return GetTypeId() == TypeId::UDT; }
            bool isUnparsed() const { return GetTypeId() == TypeId::UNPARSED; }

        protected:
            spark::connect::DataType proto_; // The underlying Protobuf message
        };

        //-------------------------------------------------------------------------
        // Concrete Simple Data Type Implementations
        //-------------------------------------------------------------------------
        class NullType : public SparkDataType
        {
        public:
            NullType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::NULL_TYPE; }
        };

        class BinaryType : public SparkDataType
        {
        public:
            BinaryType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::BINARY; }
        };

        class BooleanType : public SparkDataType
        {
        public:
            BooleanType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::BOOLEAN; }
        };

        class ByteType : public SparkDataType
        {
        public:
            ByteType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::BYTE; }
        };

        class ShortType : public SparkDataType
        {
        public:
            ShortType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::SHORT; }
        };

        class IntegerType : public SparkDataType
        {
        public:
            IntegerType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::INTEGER; }
        };

        class LongType : public SparkDataType
        {
        public:
            LongType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::LONG; }
        };

        class FloatType : public SparkDataType
        {
        public:
            FloatType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::FLOAT; }
        };

        class DoubleType : public SparkDataType
        {
        public:
            DoubleType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::DOUBLE; }
        };

        class DecimalType : public SparkDataType
        {
        public:
            DecimalType(std::optional<int32_t> precision = std::nullopt,
                        std::optional<int32_t> scale = std::nullopt,
                        uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::DECIMAL; }
            std::optional<int32_t> precision() const;
            std::optional<int32_t> scale() const;
        };

        class StringType : public SparkDataType
        {
        public:
            StringType(uint32_t type_variation_reference = 0, const std::string &collation = "");
            TypeId GetTypeId() const override { return TypeId::STRING; }
            const std::string &collation() const;
        };

        class CharType : public SparkDataType
        {
        public:
            CharType(int32_t length, uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::CHAR; }
            int32_t length() const;
        };

        class VarCharType : public SparkDataType
        {
        public:
            VarCharType(int32_t length, uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::VARCHAR; }
            int32_t length() const;
        };

        class DateType : public SparkDataType
        {
        public:
            DateType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::DATE; }
        };

        class TimestampType : public SparkDataType
        {
        public:
            TimestampType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::TIMESTAMP; }
        };

        class TimestampNTZType : public SparkDataType
        {
        public:
            TimestampNTZType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::TIMESTAMP_NTZ; }
        };

        class CalendarIntervalType : public SparkDataType
        {
        public:
            CalendarIntervalType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::CALENDAR_INTERVAL; }
        };

        class YearMonthIntervalType : public SparkDataType
        {
        public:
            YearMonthIntervalType(uint32_t type_variation_reference = 0,
                                  std::optional<int32_t> start_field = std::nullopt,
                                  std::optional<int32_t> end_field = std::nullopt);
            TypeId GetTypeId() const override { return TypeId::YEAR_MONTH_INTERVAL; }
            std::optional<int32_t> start_field() const;
            std::optional<int32_t> end_field() const;
        };

        class DayTimeIntervalType : public SparkDataType
        {
        public:
            DayTimeIntervalType(uint32_t type_variation_reference = 0,
                                std::optional<int32_t> start_field = std::nullopt,
                                std::optional<int32_t> end_field = std::nullopt);
            TypeId GetTypeId() const override { return TypeId::DAY_TIME_INTERVAL; }
            std::optional<int32_t> start_field() const;
            std::optional<int32_t> end_field() const;
        };

        //-------------------------------------------------------------------------
        // StructField Definition (not a DataType, but nested)
        //-------------------------------------------------------------------------
        class StructField
        {
        public:
            // Constructor for creating a new StructField from scratch
            StructField(std::string name, std::shared_ptr<SparkDataType> data_type, bool nullable, std::optional<std::string> metadata = std::nullopt);
            // Constructor for creating a StructField from an existing Protobuf StructField
            StructField(const spark::connect::DataType_StructField &proto_field);

            const std::string &name() const;
            const std::shared_ptr<SparkDataType> &data_type() const;
            bool nullable() const;
            std::optional<std::string> metadata() const;

            // Convert to Protobuf StructField (direct access)
            const spark::connect::DataType_StructField &ToProto() const;

        private:
            spark::connect::DataType_StructField proto_;
            std::shared_ptr<SparkDataType> data_type_; // Wrapped SparkDataType
        };

        //-------------------------------------------------------------------------
        // StructType Implementation
        //-------------------------------------------------------------------------
        class StructType : public SparkDataType
        {
        public:
            StructType(const std::vector<StructField> &fields = {}, uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::STRUCT; }
            const std::vector<StructField> &fields() const;

        private:
            std::vector<StructField> fields_; // Store C++ wrappers for convenience
        };

        //-------------------------------------------------------------------------
        // ArrayType Implementation
        //-------------------------------------------------------------------------
        class ArrayType : public SparkDataType
        {
        public:
            ArrayType(std::shared_ptr<SparkDataType> element_type, bool contains_null, uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::ARRAY; }
            const std::shared_ptr<SparkDataType> &element_type() const;
            bool contains_null() const;

        private:
            std::shared_ptr<SparkDataType> element_type_;
        };

        //-------------------------------------------------------------------------
        // MapType Implementation
        //-------------------------------------------------------------------------
        class MapType : public SparkDataType
        {
        public:
            MapType(std::shared_ptr<SparkDataType> key_type, std::shared_ptr<SparkDataType> value_type, bool value_contains_null, uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::MAP; }
            const std::shared_ptr<SparkDataType> &key_type() const;
            const std::shared_ptr<SparkDataType> &value_type() const;
            bool value_contains_null() const;

        private:
            std::shared_ptr<SparkDataType> key_type_;
            std::shared_ptr<SparkDataType> value_type_;
        };

        //-------------------------------------------------------------------------
        // VariantType Implementation
        //-------------------------------------------------------------------------
        class VariantType : public SparkDataType
        {
        public:
            VariantType(uint32_t type_variation_reference = 0);
            TypeId GetTypeId() const override { return TypeId::VARIANT; }
        };

        //-------------------------------------------------------------------------
        // UDTType Implementation
        //-------------------------------------------------------------------------
        class UDTType : public SparkDataType
        {
        public:
            UDTType(std::string type, std::shared_ptr<SparkDataType> sql_type,
                    std::optional<std::string> jvm_class = std::nullopt,
                    std::optional<std::string> python_class = std::nullopt,
                    std::optional<std::string> serialized_python_class = std::nullopt);
            TypeId GetTypeId() const override { return TypeId::UDT; }
            const std::string &type() const;
            const std::shared_ptr<SparkDataType> &sql_type() const;
            std::optional<std::string> jvm_class() const;
            std::optional<std::string> python_class() const;
            std::optional<std::string> serialized_python_class() const;

        private:
            std::shared_ptr<SparkDataType> sql_type_;
        };

        //-------------------------------------------------------------------------
        // UnparsedType Implementation
        //-------------------------------------------------------------------------
        class UnparsedType : public SparkDataType
        {
        public:
            UnparsedType(std::string data_type_string);
            TypeId GetTypeId() const override { return TypeId::UNPARSED; }
            const std::string &data_type_string() const;
        };

        //-------------------------------------------------------------------------
        // Global Convenience Functions
        //-------------------------------------------------------------------------
        std::shared_ptr<NullType> Null();
        std::shared_ptr<BooleanType> Boolean();
        std::shared_ptr<ByteType> Byte();
        std::shared_ptr<ShortType> Short();
        std::shared_ptr<IntegerType> Integer();
        std::shared_ptr<LongType> Long();
        std::shared_ptr<FloatType> Float();
        std::shared_ptr<DoubleType> Double();
        std::shared_ptr<StringType> String(const std::string &collation = "");
        std::shared_ptr<BinaryType> Binary();
        std::shared_ptr<DateType> Date();
        std::shared_ptr<TimestampType> Timestamp();
        std::shared_ptr<TimestampNTZType> TimestampNTZ();
        std::shared_ptr<CalendarIntervalType> CalendarInterval();

    } // namespace client
} // namespace spark

#endif // SPARK_CONNECT_CPP_SPARK_TYPES_H