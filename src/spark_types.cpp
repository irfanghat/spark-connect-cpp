/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "spark_types.h"
#include <utility> // For std::move

namespace spark
{
    namespace client
    {
        //------------------------------------------
        // SparkDataType Base Class Implementation
        //------------------------------------------
        uint32_t SparkDataType::type_variation_reference() const
        {
            switch (proto_.kind_case())
            {
            case spark::connect::DataType::kNull:
                return proto_.null().type_variation_reference();
            case spark::connect::DataType::kBinary:
                return proto_.binary().type_variation_reference();
            case spark::connect::DataType::kBoolean:
                return proto_.boolean().type_variation_reference();
            case spark::connect::DataType::kByte:
                return proto_.byte().type_variation_reference();
            case spark::connect::DataType::kShort:
                return proto_.short_().type_variation_reference();
            case spark::connect::DataType::kInteger:
                return proto_.integer().type_variation_reference();
            case spark::connect::DataType::kLong:
                return proto_.long_().type_variation_reference();
            case spark::connect::DataType::kFloat:
                return proto_.float_().type_variation_reference();
            case spark::connect::DataType::kDouble:
                return proto_.double_().type_variation_reference();
            case spark::connect::DataType::kDecimal:
                return proto_.decimal().type_variation_reference();
            case spark::connect::DataType::kString:
                return proto_.string().type_variation_reference(); // Using string()
            case spark::connect::DataType::kChar:
                return proto_.char_().type_variation_reference();
            case spark::connect::DataType::kVarChar:
                return proto_.var_char().type_variation_reference();
            case spark::connect::DataType::kDate:
                return proto_.date().type_variation_reference();
            case spark::connect::DataType::kTimestamp:
                return proto_.timestamp().type_variation_reference();
            case spark::connect::DataType::kTimestampNtz:
                return proto_.timestamp_ntz().type_variation_reference();
            case spark::connect::DataType::kCalendarInterval:
                return proto_.calendar_interval().type_variation_reference();
            case spark::connect::DataType::kYearMonthInterval:
                return proto_.year_month_interval().type_variation_reference();
            case spark::connect::DataType::kDayTimeInterval:
                return proto_.day_time_interval().type_variation_reference();
            case spark::connect::DataType::kArray:
                return proto_.array().type_variation_reference();
            case spark::connect::DataType::kStruct:
                return proto_.struct_().type_variation_reference();
            case spark::connect::DataType::kMap:
                return proto_.map().type_variation_reference();
            case spark::connect::DataType::kVariant:
                return proto_.variant().type_variation_reference();
            //----------------------------------------------------------------------------------
            // UDT and Unparsed don't have type_variation_reference in the same way, return 0.
            //----------------------------------------------------------------------------------
            case spark::connect::DataType::kUdt:
                return 0;
            case spark::connect::DataType::kUnparsed:
                return 0;
            case spark::connect::DataType::KIND_NOT_SET:
            default:
                return 0; // Default or unknown kind
            }
        }

        //--------------------------------------------
        // SparkDataType Factory Method
        //--------------------------------------------
        std::shared_ptr<SparkDataType> SparkDataType::FromProto(const spark::connect::DataType &proto_type)
        {
            switch (proto_type.kind_case())
            {
            case spark::connect::DataType::kNull:
                return std::make_shared<NullType>(proto_type.null().type_variation_reference());
            case spark::connect::DataType::kBinary:
                return std::make_shared<BinaryType>(proto_type.binary().type_variation_reference());
            case spark::connect::DataType::kBoolean:
                return std::make_shared<BooleanType>(proto_type.boolean().type_variation_reference());
            case spark::connect::DataType::kByte:
                return std::make_shared<ByteType>(proto_type.byte().type_variation_reference());
            case spark::connect::DataType::kShort:
                return std::make_shared<ShortType>(proto_type.short_().type_variation_reference());
            case spark::connect::DataType::kInteger:
                return std::make_shared<IntegerType>(proto_type.integer().type_variation_reference());
            case spark::connect::DataType::kLong:
                return std::make_shared<LongType>(proto_type.long_().type_variation_reference());
            case spark::connect::DataType::kFloat:
                return std::make_shared<FloatType>(proto_type.float_().type_variation_reference());
            case spark::connect::DataType::kDouble:
                return std::make_shared<DoubleType>(proto_type.double_().type_variation_reference());
            case spark::connect::DataType::kDecimal:
            {
                std::optional<int32_t> precision = proto_type.decimal().has_precision() ? std::make_optional(proto_type.decimal().precision()) : std::nullopt;
                std::optional<int32_t> scale = proto_type.decimal().has_scale() ? std::make_optional(proto_type.decimal().scale()) : std::nullopt;
                return std::make_shared<DecimalType>(precision, scale, proto_type.decimal().type_variation_reference());
            }
            case spark::connect::DataType::kString:
                return std::make_shared<StringType>(proto_type.string().type_variation_reference(), proto_type.string().collation());
            case spark::connect::DataType::kChar:
                return std::make_shared<CharType>(proto_type.char_().length(), proto_type.char_().type_variation_reference());
            case spark::connect::DataType::kVarChar:
                return std::make_shared<VarCharType>(proto_type.var_char().length(), proto_type.var_char().type_variation_reference());
            case spark::connect::DataType::kDate:
                return std::make_shared<DateType>(proto_type.date().type_variation_reference());
            case spark::connect::DataType::kTimestamp:
                return std::make_shared<TimestampType>(proto_type.timestamp().type_variation_reference());
            case spark::connect::DataType::kTimestampNtz:
                return std::make_shared<TimestampNTZType>(proto_type.timestamp_ntz().type_variation_reference());
            case spark::connect::DataType::kCalendarInterval:
                return std::make_shared<CalendarIntervalType>(proto_type.calendar_interval().type_variation_reference());
            case spark::connect::DataType::kYearMonthInterval:
            {
                std::optional<int32_t> start = proto_type.year_month_interval().has_start_field() ? std::make_optional(proto_type.year_month_interval().start_field()) : std::nullopt;
                std::optional<int32_t> end = proto_type.year_month_interval().has_end_field() ? std::make_optional(proto_type.year_month_interval().end_field()) : std::nullopt;
                return std::make_shared<YearMonthIntervalType>(proto_type.year_month_interval().type_variation_reference(), start, end);
            }
            case spark::connect::DataType::kDayTimeInterval:
            {
                std::optional<int32_t> start = proto_type.day_time_interval().has_start_field() ? std::make_optional(proto_type.day_time_interval().start_field()) : std::nullopt;
                std::optional<int32_t> end = proto_type.day_time_interval().has_end_field() ? std::make_optional(proto_type.day_time_interval().end_field()) : std::nullopt;
                return std::make_shared<DayTimeIntervalType>(proto_type.day_time_interval().type_variation_reference(), start, end);
            }
            case spark::connect::DataType::kArray:
            {
                std::shared_ptr<SparkDataType> element_type = FromProto(proto_type.array().element_type());
                return std::make_shared<ArrayType>(element_type, proto_type.array().contains_null(), proto_type.array().type_variation_reference());
            }
            case spark::connect::DataType::kStruct:
            {
                std::vector<StructField> fields;
                for (const auto &proto_field : proto_type.struct_().fields())
                {
                    fields.emplace_back(proto_field);
                }
                return std::make_shared<StructType>(fields, proto_type.struct_().type_variation_reference());
            }
            case spark::connect::DataType::kMap:
            {
                std::shared_ptr<SparkDataType> key_type = FromProto(proto_type.map().key_type());
                std::shared_ptr<SparkDataType> value_type = FromProto(proto_type.map().value_type());
                return std::make_shared<MapType>(key_type, value_type, proto_type.map().value_contains_null(), proto_type.map().type_variation_reference());
            }
            case spark::connect::DataType::kVariant:
                return std::make_shared<VariantType>(proto_type.variant().type_variation_reference());
            case spark::connect::DataType::kUdt:
            {
                std::shared_ptr<SparkDataType> sql_type = FromProto(proto_type.udt().sql_type());
                std::optional<std::string> jvm_class = proto_type.udt().has_jvm_class() ? std::make_optional(proto_type.udt().jvm_class()) : std::nullopt;
                std::optional<std::string> python_class = proto_type.udt().has_python_class() ? std::make_optional(proto_type.udt().python_class()) : std::nullopt;
                std::optional<std::string> serialized_python_class = proto_type.udt().has_serialized_python_class() ? std::make_optional(proto_type.udt().serialized_python_class()) : std::nullopt;
                return std::make_shared<UDTType>(proto_type.udt().type(), sql_type, jvm_class, python_class, serialized_python_class);
            }
            case spark::connect::DataType::kUnparsed:
                return std::make_shared<UnparsedType>(proto_type.unparsed().data_type_string());
            case spark::connect::DataType::KIND_NOT_SET:
            default:
                throw std::runtime_error("Unknown or unset Spark DataType kind in Protobuf message.");
            }
        }

        //---------------------------------------------------
        // Concrete Simple Data Type Implementations
        //---------------------------------------------------
        NullType::NullType(uint32_t type_variation_reference)
        {
            proto_.mutable_null()->set_type_variation_reference(type_variation_reference);
        }

        BinaryType::BinaryType(uint32_t type_variation_reference)
        {
            proto_.mutable_binary()->set_type_variation_reference(type_variation_reference);
        }

        BooleanType::BooleanType(uint32_t type_variation_reference)
        {
            proto_.mutable_boolean()->set_type_variation_reference(type_variation_reference);
        }

        ByteType::ByteType(uint32_t type_variation_reference)
        {
            proto_.mutable_byte()->set_type_variation_reference(type_variation_reference);
        }

        ShortType::ShortType(uint32_t type_variation_reference)
        {
            proto_.mutable_short_()->set_type_variation_reference(type_variation_reference);
        }

        IntegerType::IntegerType(uint32_t type_variation_reference)
        {
            proto_.mutable_integer()->set_type_variation_reference(type_variation_reference);
        }

        LongType::LongType(uint32_t type_variation_reference)
        {
            proto_.mutable_long_()->set_type_variation_reference(type_variation_reference);
        }

        FloatType::FloatType(uint32_t type_variation_reference)
        {
            proto_.mutable_float_()->set_type_variation_reference(type_variation_reference);
        }

        DoubleType::DoubleType(uint32_t type_variation_reference)
        {
            proto_.mutable_double_()->set_type_variation_reference(type_variation_reference);
        }

        StringType::StringType(uint32_t type_variation_reference, const std::string &collation)
        {
            proto_.mutable_string()->set_type_variation_reference(type_variation_reference);
            proto_.mutable_string()->set_collation(collation);
        }
        const std::string &StringType::collation() const { return proto_.string().collation(); }

        DateType::DateType(uint32_t type_variation_reference)
        {
            proto_.mutable_date()->set_type_variation_reference(type_variation_reference);
        }

        TimestampType::TimestampType(uint32_t type_variation_reference)
        {
            proto_.mutable_timestamp()->set_type_variation_reference(type_variation_reference);
        }

        TimestampNTZType::TimestampNTZType(uint32_t type_variation_reference)
        {
            proto_.mutable_timestamp_ntz()->set_type_variation_reference(type_variation_reference);
        }

        CalendarIntervalType::CalendarIntervalType(uint32_t type_variation_reference)
        {
            proto_.mutable_calendar_interval()->set_type_variation_reference(type_variation_reference);
        }

        //------------------------------------------------------
        // Concrete Complex Data Type Implementations
        //------------------------------------------------------
        YearMonthIntervalType::YearMonthIntervalType(uint32_t type_variation_reference,
                                                     std::optional<int32_t> start_field,
                                                     std::optional<int32_t> end_field)
        {
            auto ym_proto = proto_.mutable_year_month_interval();
            ym_proto->set_type_variation_reference(type_variation_reference);
            if (start_field.has_value())
            {
                ym_proto->set_start_field(start_field.value());
            }
            if (end_field.has_value())
            {
                ym_proto->set_end_field(end_field.value());
            }
        }
        std::optional<int32_t> YearMonthIntervalType::start_field() const
        {
            if (proto_.year_month_interval().has_start_field())
            {
                return proto_.year_month_interval().start_field();
            }
            return std::nullopt;
        }
        std::optional<int32_t> YearMonthIntervalType::end_field() const
        {
            if (proto_.year_month_interval().has_end_field())
            {
                return proto_.year_month_interval().end_field();
            }
            return std::nullopt;
        }

        DayTimeIntervalType::DayTimeIntervalType(uint32_t type_variation_reference,
                                                 std::optional<int32_t> start_field,
                                                 std::optional<int32_t> end_field)
        {
            auto dt_proto = proto_.mutable_day_time_interval();
            dt_proto->set_type_variation_reference(type_variation_reference);
            if (start_field.has_value())
            {
                dt_proto->set_start_field(start_field.value());
            }
            if (end_field.has_value())
            {
                dt_proto->set_end_field(end_field.value());
            }
        }
        std::optional<int32_t> DayTimeIntervalType::start_field() const
        {
            if (proto_.day_time_interval().has_start_field())
            {
                return proto_.day_time_interval().start_field();
            }
            return std::nullopt;
        }
        std::optional<int32_t> DayTimeIntervalType::end_field() const
        {
            if (proto_.day_time_interval().has_end_field())
            {
                return proto_.day_time_interval().end_field();
            }
            return std::nullopt;
        }

        CharType::CharType(int32_t length, uint32_t type_variation_reference)
        {
            auto char_proto = proto_.mutable_char_();
            char_proto->set_length(length);
            char_proto->set_type_variation_reference(type_variation_reference);
        }
        int32_t CharType::length() const { return proto_.char_().length(); }

        VarCharType::VarCharType(int32_t length, uint32_t type_variation_reference)
        {
            auto varchar_proto = proto_.mutable_var_char();
            varchar_proto->set_length(length);
            varchar_proto->set_type_variation_reference(type_variation_reference);
        }
        int32_t VarCharType::length() const { return proto_.var_char().length(); }

        DecimalType::DecimalType(std::optional<int32_t> precision,
                                 std::optional<int32_t> scale,
                                 uint32_t type_variation_reference)
        {
            auto decimal_proto = proto_.mutable_decimal();
            decimal_proto->set_type_variation_reference(type_variation_reference);
            if (precision.has_value())
            {
                decimal_proto->set_precision(precision.value());
            }
            if (scale.has_value())
            {
                decimal_proto->set_scale(scale.value());
            }
        }
        std::optional<int32_t> DecimalType::precision() const
        {
            if (proto_.decimal().has_precision())
            {
                return proto_.decimal().precision();
            }
            return std::nullopt;
        }
        std::optional<int32_t> DecimalType::scale() const
        {
            if (proto_.decimal().has_scale())
            {
                return proto_.decimal().scale();
            }
            return std::nullopt;
        }

        //-----------------------------------
        // StructField Implementation
        //-----------------------------------
        StructField::StructField(std::string name, std::shared_ptr<SparkDataType> data_type, bool nullable, std::optional<std::string> metadata)
            : data_type_(std::move(data_type))
        {
            proto_.set_name(name);
            proto_.set_nullable(nullable);
            *proto_.mutable_data_type() = data_type_->ToProto(); // Copy the underlying proto message
            if (metadata.has_value())
            {
                proto_.set_metadata(metadata.value());
            }
        }

        StructField::StructField(const spark::connect::DataType_StructField &proto_field)
            : proto_(proto_field), data_type_(SparkDataType::FromProto(proto_field.data_type())) {}

        const std::string &StructField::name() const { return proto_.name(); }
        const std::shared_ptr<SparkDataType> &StructField::data_type() const { return data_type_; }
        bool StructField::nullable() const { return proto_.nullable(); }
        std::optional<std::string> StructField::metadata() const
        {
            if (proto_.has_metadata())
            {
                return proto_.metadata();
            }
            return std::nullopt;
        }
        const spark::connect::DataType_StructField &StructField::ToProto() const { return proto_; }

        //--------------------------------------
        // StructType Implementation
        //--------------------------------------
        StructType::StructType(const std::vector<StructField> &fields, uint32_t type_variation_reference)
            : fields_(fields)
        {
            auto struct_proto = proto_.mutable_struct_(); // Using struct_()
            struct_proto->set_type_variation_reference(type_variation_reference);
            for (const auto &field : fields_)
            {
                *struct_proto->add_fields() = field.ToProto();
            }
        }
        const std::vector<StructField> &StructType::fields() const { return fields_; }

        //------------------------------------
        // ArrayType Implementation
        //------------------------------------
        ArrayType::ArrayType(std::shared_ptr<SparkDataType> element_type, bool contains_null, uint32_t type_variation_reference)
            : element_type_(std::move(element_type))
        {
            auto array_proto = proto_.mutable_array();
            array_proto->set_type_variation_reference(type_variation_reference);
            *array_proto->mutable_element_type() = element_type_->ToProto();
            array_proto->set_contains_null(contains_null);
        }
        const std::shared_ptr<SparkDataType> &ArrayType::element_type() const { return element_type_; }
        bool ArrayType::contains_null() const { return proto_.array().contains_null(); }

        //--------------------------------
        // MapType Implementation
        //--------------------------------
        MapType::MapType(std::shared_ptr<SparkDataType> key_type, std::shared_ptr<SparkDataType> value_type, bool value_contains_null, uint32_t type_variation_reference)
            : key_type_(std::move(key_type)), value_type_(std::move(value_type))
        {
            auto map_proto = proto_.mutable_map();
            map_proto->set_type_variation_reference(type_variation_reference);
            *map_proto->mutable_key_type() = key_type_->ToProto();
            *map_proto->mutable_value_type() = value_type_->ToProto();
            map_proto->set_value_contains_null(value_contains_null);
        }
        const std::shared_ptr<SparkDataType> &MapType::key_type() const { return key_type_; }
        const std::shared_ptr<SparkDataType> &MapType::value_type() const { return value_type_; }
        bool MapType::value_contains_null() const { return proto_.map().value_contains_null(); }

        //-------------------------------------
        // VariantType Implementation
        //-------------------------------------
        VariantType::VariantType(uint32_t type_variation_reference)
        {
            proto_.mutable_variant()->set_type_variation_reference(type_variation_reference);
        }

        //------------------------------------
        // UDTType Implementation
        //------------------------------------
        UDTType::UDTType(std::string type, std::shared_ptr<SparkDataType> sql_type,
                         std::optional<std::string> jvm_class,
                         std::optional<std::string> python_class,
                         std::optional<std::string> serialized_python_class)
            : sql_type_(std::move(sql_type))
        {
            auto udt_proto = proto_.mutable_udt();
            udt_proto->set_type(type);
            *udt_proto->mutable_sql_type() = sql_type_->ToProto();
            if (jvm_class.has_value())
            {
                udt_proto->set_jvm_class(jvm_class.value());
            }
            if (python_class.has_value())
            {
                udt_proto->set_python_class(python_class.value());
            }
            if (serialized_python_class.has_value())
            {
                udt_proto->set_serialized_python_class(serialized_python_class.value());
            }
        }
        const std::string &UDTType::type() const { return proto_.udt().type(); }
        const std::shared_ptr<SparkDataType> &UDTType::sql_type() const { return sql_type_; }
        std::optional<std::string> UDTType::jvm_class() const
        {
            if (proto_.udt().has_jvm_class())
            {
                return proto_.udt().jvm_class();
            }
            return std::nullopt;
        }
        std::optional<std::string> UDTType::python_class() const
        {
            if (proto_.udt().has_python_class())
            {
                return proto_.udt().python_class();
            }
            return std::nullopt;
        }
        std::optional<std::string> UDTType::serialized_python_class() const
        {
            if (proto_.udt().has_serialized_python_class())
            {
                return proto_.udt().serialized_python_class();
            }
            return std::nullopt;
        }

        //------------------------------------
        // UnparsedType Implementation
        //------------------------------------
        UnparsedType::UnparsedType(std::string data_type_string)
        {
            proto_.mutable_unparsed()->set_data_type_string(data_type_string);
        }
        const std::string &UnparsedType::data_type_string() const { return proto_.unparsed().data_type_string(); }

        //------------------------------------
        // Global Convenience Functions
        //------------------------------------
        std::shared_ptr<NullType> Null() { return std::make_shared<NullType>(); }
        std::shared_ptr<BooleanType> Boolean() { return std::make_shared<BooleanType>(); }
        std::shared_ptr<ByteType> Byte() { return std::make_shared<ByteType>(); }
        std::shared_ptr<ShortType> Short() { return std::make_shared<ShortType>(); }
        std::shared_ptr<IntegerType> Integer() { return std::make_shared<IntegerType>(); }
        std::shared_ptr<LongType> Long() { return std::make_shared<LongType>(); }
        std::shared_ptr<FloatType> Float() { return std::make_shared<FloatType>(); }
        std::shared_ptr<DoubleType> Double() { return std::make_shared<DoubleType>(); }
        std::shared_ptr<StringType> String(const std::string &collation) { return std::make_shared<StringType>(0, collation); }
        std::shared_ptr<BinaryType> Binary() { return std::make_shared<BinaryType>(); }
        std::shared_ptr<DateType> Date() { return std::make_shared<DateType>(); }
        std::shared_ptr<TimestampType> Timestamp() { return std::make_shared<TimestampType>(); }
        std::shared_ptr<TimestampNTZType> TimestampNTZ() { return std::make_shared<TimestampNTZType>(); }
        std::shared_ptr<CalendarIntervalType> CalendarInterval() { return std::make_shared<CalendarIntervalType>(); }

    } // namespace client
} // namespace spark