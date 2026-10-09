#include "types/DataType.h"

#include <spark/connect/types.pb.h>

namespace spark::sql::types
{
// ---------------------------------------------------------
// Protobuf to C++ Conversion Logic
// ---------------------------------------------------------

static DataType
from_proto_internal(spark::connect::DataType const& proto)
{
    using namespace spark::connect;

    if (proto.has_null())
        return DataType(NullType{});
    if (proto.has_boolean())
        return DataType(BooleanType{});
    if (proto.has_byte())
        return DataType(ByteType{});
    if (proto.has_short_())
        return DataType(ShortType{});
    if (proto.has_integer())
        return DataType(IntegerType{});
    if (proto.has_long_())
        return DataType(LongType{});
    if (proto.has_float_())
        return DataType(FloatType{});
    if (proto.has_double_())
        return DataType(DoubleType{});
    if (proto.has_string())
        return DataType(StringType{});
    if (proto.has_binary())
        return DataType(BinaryType{});
    if (proto.has_date())
        return DataType(DateType{});
    if (proto.has_timestamp())
        return DataType(TimestampType{});
    if (proto.has_timestamp_ntz())
        return DataType(TimestampNtzType{});

    if (proto.has_decimal())
    {
        return DataType(
            DecimalType{proto.decimal().precision(), proto.decimal().scale()});
    }
    if (proto.has_char_())
    {
        return DataType(CharType{proto.char_().length()});
    }
    if (proto.has_var_char())
    {
        return DataType(VarCharType{proto.var_char().length()});
    }

    if (proto.has_array())
    {
        ArrayType arr;
        arr.element_type = std::make_shared<DataType>(
            from_proto_internal(proto.array().element_type()));
        arr.contains_null = proto.array().contains_null();
        return DataType(arr);
    }

    if (proto.has_map())
    {
        MapType map;
        map.key_type = std::make_shared<DataType>(
            from_proto_internal(proto.map().key_type()));
        map.value_type = std::make_shared<DataType>(
            from_proto_internal(proto.map().value_type()));
        map.value_contains_null = proto.map().value_contains_null();
        return DataType(map);
    }

    if (proto.has_struct_())
    {
        StructType st;
        for (auto const& field : proto.struct_().fields())
        {
            st.fields.push_back({field.name(),
                                 std::make_shared<DataType>(
                                     from_proto_internal(field.data_type())),
                                 field.nullable()});
        }
        return DataType(st);
    }

    // -----------------------------
    // Fall back to NullType
    // -----------------------------
    return DataType(NullType{});
}

DataType
DataType::from_proto(spark::connect::DataType const& proto)
{
    return from_proto_internal(proto);
}

// ---------------------------------------------------------
// JSON Serialization Logic
// ---------------------------------------------------------
struct JsonVisitor
{
    std::string
    operator()(NullType const&) const
    {
        return "\"void\"";
    }
    std::string
    operator()(BooleanType const&) const
    {
        return "\"boolean\"";
    }
    std::string
    operator()(ByteType const&) const
    {
        return "\"byte\"";
    }
    std::string
    operator()(ShortType const&) const
    {
        return "\"short\"";
    }
    std::string
    operator()(IntegerType const&) const
    {
        return "\"integer\"";
    }
    std::string
    operator()(LongType const&) const
    {
        return "\"long\"";
    }
    std::string
    operator()(FloatType const&) const
    {
        return "\"float\"";
    }
    std::string
    operator()(DoubleType const&) const
    {
        return "\"double\"";
    }
    std::string
    operator()(StringType const&) const
    {
        return "\"string\"";
    }
    std::string
    operator()(BinaryType const&) const
    {
        return "\"binary\"";
    }
    std::string
    operator()(DateType const&) const
    {
        return "\"date\"";
    }
    std::string
    operator()(TimestampType const&) const
    {
        return "\"timestamp\"";
    }
    std::string
    operator()(TimestampNtzType const&) const
    {
        return "\"timestamp_ntz\"";
    }

    std::string
    operator()(DecimalType const& t) const
    {
        return "\"decimal(" + std::to_string(t.precision) + "," +
               std::to_string(t.scale) + ")\"";
    }
    std::string
    operator()(CharType const& t) const
    {
        return "\"char(" + std::to_string(t.length) + ")\"";
    }
    std::string
    operator()(VarCharType const& t) const
    {
        return "\"varchar(" + std::to_string(t.length) + ")\"";
    }

    std::string
    operator()(ArrayType const& t) const
    {
        return "{\"type\":\"array\",\"elementType\":" + t.element_type->json() +
               ",\"containsNull\":" + (t.contains_null ? "true" : "false") +
               "}";
    }

    std::string
    operator()(MapType const& t) const
    {
        return "{\"type\":\"map\",\"keyType\":" + t.key_type->json() +
               ",\"valueType\":" + t.value_type->json() +
               ",\"valueContainsNull\":" +
               (t.value_contains_null ? "true" : "false") + "}";
    }

    std::string
    operator()(StructType const& t) const
    {
        return t.json();
    }
};

std::string
DataType::json() const
{
    return std::visit(JsonVisitor{}, kind);
}

struct TypeNameVisitor
{
    std::string
    operator()(NullType const&) const
    {
        return "void";
    }
    std::string
    operator()(BooleanType const&) const
    {
        return "boolean";
    }
    std::string
    operator()(ByteType const&) const
    {
        return "byte";
    }
    std::string
    operator()(ShortType const&) const
    {
        return "short";
    }
    std::string
    operator()(IntegerType const&) const
    {
        return "integer";
    }
    std::string
    operator()(LongType const&) const
    {
        return "long";
    }
    std::string
    operator()(FloatType const&) const
    {
        return "float";
    }
    std::string
    operator()(DoubleType const&) const
    {
        return "double";
    }
    std::string
    operator()(StringType const&) const
    {
        return "string";
    }
    std::string
    operator()(BinaryType const&) const
    {
        return "binary";
    }
    std::string
    operator()(DateType const&) const
    {
        return "date";
    }
    std::string
    operator()(TimestampType const&) const
    {
        return "timestamp";
    }
    std::string
    operator()(TimestampNtzType const&) const
    {
        return "timestamp_ntz";
    }
    std::string
    operator()(DecimalType const&) const
    {
        return "decimal";
    }
    std::string
    operator()(CharType const&) const
    {
        return "char";
    }
    std::string
    operator()(VarCharType const&) const
    {
        return "varchar";
    }
    std::string
    operator()(ArrayType const&) const
    {
        return "array";
    }
    std::string
    operator()(MapType const&) const
    {
        return "map";
    }
    std::string
    operator()(StructType const&) const
    {
        return "struct";
    }
};

std::string
DataType::type_name() const
{
    return std::visit(TypeNameVisitor{}, kind);
}

} // namespace spark::sql::types
