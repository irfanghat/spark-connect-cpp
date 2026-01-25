#include "types.h"
#include <sstream>
#include <spark/connect/types.pb.h>

namespace spark::sql::types
{
    // ---------------------------------------------------------
    // Protobuf to C++ Conversion Logic (Internal)
    // ---------------------------------------------------------

    /**
     * @brief
     * The following is a static helper for recursive type resolution
     */
    static DataType from_proto_internal(const spark::connect::DataType &proto)
    {
        using namespace spark::connect;

        // -------------------------
        // Check for Simple Types
        // -------------------------
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

        // ---------------------------------
        // Check for Parameterized Types
        // ---------------------------------
        if (proto.has_decimal())
        {
            return DataType(DecimalType{proto.decimal().precision(), proto.decimal().scale()});
        }
        if (proto.has_char_())
        {
            return DataType(CharType{proto.char_().length()});
        }
        if (proto.has_var_char())
        {
            return DataType(VarCharType{proto.var_char().length()});
        }

        // ---------------------------------
        // Check for Complex Types
        // ---------------------------------
        if (proto.has_array())
        {
            ArrayType arr;
            arr.element_type = std::make_shared<DataType>(from_proto_internal(proto.array().element_type()));
            arr.contains_null = proto.array().contains_null();
            return DataType(arr);
        }

        if (proto.has_map())
        {
            MapType map;
            map.key_type = std::make_shared<DataType>(from_proto_internal(proto.map().key_type()));
            map.value_type = std::make_shared<DataType>(from_proto_internal(proto.map().value_type()));
            map.value_contains_null = proto.map().value_contains_null();
            return DataType(map);
        }

        if (proto.has_struct_())
        {
            StructType st;
            for (const auto &field : proto.struct_().fields())
            {
                st.fields.push_back({field.name(),
                                     from_proto_internal(field.data_type()),
                                     field.nullable()});
            }
            return DataType(st);
        }

        // Fall back to NullType
        return DataType(NullType{});
    }

    /**
     * @brief
     * Public factory method
     */
    DataType DataType::from_proto(const spark::connect::DataType &proto)
    {
        return from_proto_internal(proto);
    }

    /**
     * @brief
     * JSON Serialization Logic
     */
    struct JsonVisitor
    {
        std::string operator()(const NullType &) const { return "\"void\""; }
        std::string operator()(const BooleanType &) const { return "\"boolean\""; }
        std::string operator()(const ByteType &) const { return "\"byte\""; }
        std::string operator()(const ShortType &) const { return "\"short\""; }
        std::string operator()(const IntegerType &) const { return "\"integer\""; }
        std::string operator()(const LongType &) const { return "\"long\""; }
        std::string operator()(const FloatType &) const { return "\"float\""; }
        std::string operator()(const DoubleType &) const { return "\"double\""; }
        std::string operator()(const StringType &) const { return "\"string\""; }
        std::string operator()(const BinaryType &) const { return "\"binary\""; }
        std::string operator()(const DateType &) const { return "\"date\""; }
        std::string operator()(const TimestampType &) const { return "\"timestamp\""; }
        std::string operator()(const TimestampNtzType &) const { return "\"timestamp_ntz\""; }

        std::string operator()(const DecimalType &t) const
        {
            return "\"decimal(" + std::to_string(t.precision) + "," + std::to_string(t.scale) + ")\"";
        }
        std::string operator()(const CharType &t) const
        {
            return "\"char(" + std::to_string(t.length) + ")\"";
        }
        std::string operator()(const VarCharType &t) const
        {
            return "\"varchar(" + std::to_string(t.length) + ")\"";
        }

        std::string operator()(const ArrayType &t) const
        {
            return "{\"type\":\"array\",\"elementType\":" + t.element_type->json() +
                   ",\"containsNull\":" + (t.contains_null ? "true" : "false") + "}";
        }

        std::string operator()(const MapType &t) const
        {
            return "{\"type\":\"map\",\"keyType\":" + t.key_type->json() +
                   ",\"valueType\":" + t.value_type->json() +
                   ",\"valueContainsNull\":" + (t.value_contains_null ? "true" : "false") + "}";
        }

        std::string operator()(const StructType &t) const
        {
            std::string out = "{\"type\":\"struct\",\"fields\":[";
            for (size_t i = 0; i < t.fields.size(); ++i)
            {
                const auto &f = t.fields[i];
                out += "{\"name\":\"" + f.name + "\",\"type\":" + f.data_type.json() +
                       ",\"nullable\":" + (f.nullable ? "true" : "false") + ",\"metadata\":{}}";
                if (i < t.fields.size() - 1)
                    out += ",";
            }
            out += "]}";
            return out;
        }
    };

    std::string DataType::json() const
    {
        return std::visit(JsonVisitor{}, kind);
    }

    std::string StructType::json() const
    {
        return JsonVisitor{}(*this);
    }

    /**
     * @brief
     * Internal visitor for tree printing
     */
    struct TreeVisitor
    {
        std::ostream &os;
        int level;

        void operator()(const NullType &) const { os << "void"; }
        void operator()(const BooleanType &) const { os << "boolean"; }
        void operator()(const ByteType &) const { os << "byte"; }
        void operator()(const ShortType &) const { os << "short"; }
        void operator()(const IntegerType &) const { os << "integer"; }
        void operator()(const LongType &) const { os << "long"; }
        void operator()(const FloatType &) const { os << "float"; }
        void operator()(const DoubleType &) const { os << "double"; }
        void operator()(const StringType &) const { os << "string"; }
        void operator()(const BinaryType &) const { os << "binary"; }
        void operator()(const DateType &) const { os << "date"; }
        void operator()(const TimestampType &) const { os << "timestamp"; }
        void operator()(const TimestampNtzType &) const { os << "timestamp_ntz"; }

        void operator()(const DecimalType &t) const
        {
            os << "decimal(" << t.precision << "," << t.scale << ")";
        }
        void operator()(const CharType &t) const
        {
            os << "char(" << t.length << ")";
        }
        void operator()(const VarCharType &t) const
        {
            os << "varchar(" << t.length << ")";
        }

        void operator()(const ArrayType &t) const
        {
            os << "array" << std::endl;
            print_indent(level + 1);
            os << "element: ";
            std::visit(TreeVisitor{os, level + 1}, t.element_type->kind);
        }

        void operator()(const MapType &t) const
        {
            os << "map" << std::endl;
            print_indent(level + 1);
            os << "key: ";
            std::visit(TreeVisitor{os, level + 1}, t.key_type->kind);
            os << std::endl;
            print_indent(level + 1);
            os << "value: ";
            std::visit(TreeVisitor{os, level + 1}, t.value_type->kind);
        }

        void operator()(const StructType &t) const
        {
            os << "struct" << std::endl;
            for (const auto &f : t.fields)
            {
                print_indent(level + 1);
                os << f.name << ": ";
                std::visit(TreeVisitor{os, level + 1}, f.data_type.kind);
                os << " (nullable = " << (f.nullable ? "true" : "false") << ")" << std::endl;
            }
        }

    private:
        void print_indent(int l) const
        {
            for (int i = 0; i < l - 1; ++i)
                os << " |  ";
            os << " |-- ";
        }
    };

    void StructType::print_tree(std::ostream &os) const
    {
        os << "root" << std::endl;
        for (const auto &f : fields)
        {
            os << " |-- " << f.name << ": ";
            std::visit(TreeVisitor{os, 1}, f.data_type.kind);
            os << " (nullable = " << (f.nullable ? "true" : "false") << ")" << std::endl;
        }
    }
}