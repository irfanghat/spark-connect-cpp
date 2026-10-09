#include "types/StructType.h"

#include "types/DataType.h"

#include <iostream>

namespace spark::sql::types
{
struct TreeVisitor
{
    std::ostream& os;
    int level;

    void
    operator()(NullType const&) const
    {
        os << "void";
    }
    void
    operator()(BooleanType const&) const
    {
        os << "boolean";
    }
    void
    operator()(ByteType const&) const
    {
        os << "byte";
    }
    void
    operator()(ShortType const&) const
    {
        os << "short";
    }
    void
    operator()(IntegerType const&) const
    {
        os << "integer";
    }
    void
    operator()(LongType const&) const
    {
        os << "long";
    }
    void
    operator()(FloatType const&) const
    {
        os << "float";
    }
    void
    operator()(DoubleType const&) const
    {
        os << "double";
    }
    void
    operator()(StringType const&) const
    {
        os << "string";
    }
    void
    operator()(BinaryType const&) const
    {
        os << "binary";
    }
    void
    operator()(DateType const&) const
    {
        os << "date";
    }
    void
    operator()(TimestampType const&) const
    {
        os << "timestamp";
    }
    void
    operator()(TimestampNtzType const&) const
    {
        os << "timestamp_ntz";
    }

    void
    operator()(DecimalType const& t) const
    {
        os << "decimal(" << t.precision << "," << t.scale << ")";
    }
    void
    operator()(CharType const& t) const
    {
        os << "char(" << t.length << ")";
    }
    void
    operator()(VarCharType const& t) const
    {
        os << "varchar(" << t.length << ")";
    }

    void
    operator()(ArrayType const& t) const
    {
        os << "array" << std::endl;
        print_indent(level + 1);
        os << "element: ";
        std::visit(TreeVisitor{os, level + 1}, t.element_type->kind);
    }

    void
    operator()(MapType const& t) const
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

    void
    operator()(StructType const& t) const
    {
        os << "struct" << std::endl;
        for (auto const& f : t.fields)
        {
            print_indent(level + 1);
            os << f.name << ": ";
            std::visit(TreeVisitor{os, level + 1}, f.data_type->kind);
            os << " (nullable = " << (f.nullable ? "true" : "false") << ")"
               << std::endl;
        }
    }

  private:
    void
    print_indent(int l) const
    {
        for (int i = 0; i < l - 1; ++i)
            os << " |  ";
        os << " |-- ";
    }
};

std::string
StructType::json() const
{
    std::string out = "{\"type\":\"struct\",\"fields\":[";
    for (size_t i = 0; i < fields.size(); ++i)
    {
        auto const& f = fields[i];
        out += "{\"name\":\"" + f.name + "\",\"type\":" + f.data_type->json() +
               ",\"nullable\":" + (f.nullable ? "true" : "false") +
               ",\"metadata\":{}}";
        if (i < fields.size() - 1)
            out += ",";
    }
    out += "]}";
    return out;
}

void
StructType::print_tree(std::ostream& os) const
{
    os << "root" << std::endl;
    for (auto const& f : fields)
    {
        os << " |-- " << f.name << ": ";
        std::visit(TreeVisitor{os, 1}, f.data_type->kind);
        os << " (nullable = " << (f.nullable ? "true" : "false") << ")"
           << std::endl;
    }
}

} // namespace spark::sql::types
