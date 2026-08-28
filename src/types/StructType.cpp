#include "types/StructType.h"
#include "types/DataType.h"

#include <iostream>

namespace spark::sql::types
{
struct TreeVisitor
{
    std::ostream& os;
    int level;

    void operator()(const NullType&) const
    {
        os << "void";
    }
    void operator()(const BooleanType&) const
    {
        os << "boolean";
    }
    void operator()(const ByteType&) const
    {
        os << "byte";
    }
    void operator()(const ShortType&) const
    {
        os << "short";
    }
    void operator()(const IntegerType&) const
    {
        os << "integer";
    }
    void operator()(const LongType&) const
    {
        os << "long";
    }
    void operator()(const FloatType&) const
    {
        os << "float";
    }
    void operator()(const DoubleType&) const
    {
        os << "double";
    }
    void operator()(const StringType&) const
    {
        os << "string";
    }
    void operator()(const BinaryType&) const
    {
        os << "binary";
    }
    void operator()(const DateType&) const
    {
        os << "date";
    }
    void operator()(const TimestampType&) const
    {
        os << "timestamp";
    }
    void operator()(const TimestampNtzType&) const
    {
        os << "timestamp_ntz";
    }

    void operator()(const DecimalType& t) const
    {
        os << "decimal(" << t.precision << "," << t.scale << ")";
    }
    void operator()(const CharType& t) const
    {
        os << "char(" << t.length << ")";
    }
    void operator()(const VarCharType& t) const
    {
        os << "varchar(" << t.length << ")";
    }

    void operator()(const ArrayType& t) const
    {
        os << "array" << std::endl;
        print_indent(level + 1);
        os << "element: ";
        std::visit(TreeVisitor{os, level + 1}, t.element_type->kind);
    }

    void operator()(const MapType& t) const
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

    void operator()(const StructType& t) const
    {
        os << "struct" << std::endl;
        for (const auto& f : t.fields)
        {
            print_indent(level + 1);
            os << f.name << ": ";
            std::visit(TreeVisitor{os, level + 1}, f.data_type->kind);
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

std::string StructType::json() const
{
    std::string out = "{\"type\":\"struct\",\"fields\":[";
    for (size_t i = 0; i < fields.size(); ++i)
    {
        const auto& f = fields[i];
        out += "{\"name\":\"" + f.name + "\",\"type\":" + f.data_type->json() +
               ",\"nullable\":" + (f.nullable ? "true" : "false") + ",\"metadata\":{}}";
        if (i < fields.size() - 1)
            out += ",";
    }
    out += "]}";
    return out;
}

void StructType::print_tree(std::ostream& os) const
{
    os << "root" << std::endl;
    for (const auto& f : fields)
    {
        os << " |-- " << f.name << ": ";
        std::visit(TreeVisitor{os, 1}, f.data_type->kind);
        os << " (nullable = " << (f.nullable ? "true" : "false") << ")" << std::endl;
    }
}

} // namespace spark::sql::types

