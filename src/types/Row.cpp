#include "types/Row.h"

#include <iomanip>
#include <iostream>

#include <arrow/array.h>

namespace spark::sql::types
{
/**
 * @brief A bridge for Arrow to C++ variant conversion
 */
ColumnValue arrayValueToVariant(const std::shared_ptr<arrow::Array>& array, int64_t row)
{
    if (array->IsNull(row))
        return std::monostate{};

    switch (array->type_id())
    {
    case arrow::Type::BOOL:
        return std::static_pointer_cast<arrow::BooleanArray>(array)->Value(row);
    case arrow::Type::INT8:
        return static_cast<int8_t>(std::static_pointer_cast<arrow::Int8Array>(array)->Value(row));
    case arrow::Type::INT16:
        return static_cast<int16_t>(std::static_pointer_cast<arrow::Int16Array>(array)->Value(row));
    case arrow::Type::INT32:
    case arrow::Type::DATE32:
        return std::static_pointer_cast<arrow::Int32Array>(array)->Value(row);
    case arrow::Type::INT64:
    case arrow::Type::TIMESTAMP:
    case arrow::Type::DATE64:
        return std::static_pointer_cast<arrow::Int64Array>(array)->Value(row);
    case arrow::Type::FLOAT:
        return std::static_pointer_cast<arrow::FloatArray>(array)->Value(row);
    case arrow::Type::DOUBLE:
        return std::static_pointer_cast<arrow::DoubleArray>(array)->Value(row);
    case arrow::Type::STRING:
        return std::static_pointer_cast<arrow::StringArray>(array)->GetString(row);
    case arrow::Type::BINARY:
    {
        auto bin_arr = std::static_pointer_cast<arrow::BinaryArray>(array);
        auto view = bin_arr->GetView(row);
        return std::vector<uint8_t>(view.begin(), view.end());
    }
    case arrow::Type::LIST:
    {
        auto list_array = std::static_pointer_cast<arrow::ListArray>(array);
        auto value_array = list_array->values();
        auto out_array = std::make_shared<ArrayData>();
        for (int64_t i = list_array->value_offset(row); i < list_array->value_offset(row + 1); ++i)
        {
            out_array->elements.push_back(arrayValueToVariant(value_array, i));
        }
        return out_array;
    }
    case arrow::Type::STRUCT:
    {
        auto struct_array = std::static_pointer_cast<arrow::StructArray>(array);
        auto out_row = std::make_shared<Row>();
        for (int i = 0; i < struct_array->num_fields(); ++i)
        {
            out_row->column_names.push_back(struct_array->struct_type()->field(i)->name());
            out_row->values.push_back(arrayValueToVariant(struct_array->field(i), row));
        }
        return out_row;
    }
    case arrow::Type::MAP:
    {
        // ------------------------------------------------------
        // Arrow Maps are structured as List<Struct<key, value>>
        // ------------------------------------------------------
        auto map_array = std::static_pointer_cast<arrow::MapArray>(array);
        auto struct_array = std::static_pointer_cast<arrow::StructArray>(map_array->values());
        auto keys = struct_array->field(0);
        auto values = struct_array->field(1);
        auto out_map = std::make_shared<MapData>();
        for (int64_t i = map_array->value_offset(row); i < map_array->value_offset(row + 1); ++i)
        {
            out_map->keys.push_back(arrayValueToVariant(keys, i));
            out_map->values.push_back(arrayValueToVariant(values, i));
        }
        return out_map;
    }
    default:
        return std::monostate{};
    }
}

// ---------------------------------------------------------
// Row Visualization Logic
// ---------------------------------------------------------
struct RowStringVisitor
{
    std::ostream& os;

    void operator()(std::monostate) const
    {
        os << "null";
    }
    void operator()(const std::string& v) const
    {
        os << "'" << v << "'";
    }
    void operator()(bool v) const
    {
        os << (v ? "true" : "false");
    }

    /**
     * @brief Binary Visualization (Hex Dump)
     */
    void operator()(const std::vector<uint8_t>& v) const
    {
        os << "0x" << std::hex << std::setfill('0');
        for (auto b : v)
            os << std::setw(2) << static_cast<int>(b);
        os << std::dec;
    }

    /**
     * @brief Reset to decimal for subsequent prints
     */
    void operator()(const std::shared_ptr<MapData>& v) const
    {
        os << "{";
        for (size_t i = 0; i < v->keys.size(); ++i)
        {
            std::visit(*this, v->keys[i]);
            os << ": ";
            std::visit(*this, v->values[i]);
            if (i < v->keys.size() - 1)
                os << ", ";
        }
        os << "}";
    }

    /**
     * @brief Map Support
     */
    void operator()(const std::shared_ptr<Row>& v) const
    {
        if (v)
            os << *v;
        else
            os << "null";
    }

    /**
     *  @brief This handles Recursive Types such as Row and Array
     */
    void operator()(const std::shared_ptr<ArrayData>& v) const
    {
        if (!v)
        {
            os << "null";
            return;
        }
        os << "[";
        for (size_t i = 0; i < v->elements.size(); ++i)
        {
            std::visit(*this, v->elements[i]);
            if (i < v->elements.size() - 1)
                os << ", ";
        }
        os << "]";
    }

    // -------------------------------------------------
    // Fallback for Primitives (int, double, etc.)
    // -------------------------------------------------
    template <typename T> void operator()(const T& v) const
    {
        os << v;
    }
};

std::ostream& operator<<(std::ostream& os, const Row& row)
{
    os << "Row(";
    for (size_t i = 0; i < row.values.size(); ++i)
    {
        os << row.column_names[i] << "=";
        std::visit(RowStringVisitor{os}, row.values[i]);
        if (i < row.values.size() - 1)
            os << ", ";
    }
    os << ")";
    return os;
}

} // namespace spark::sql::types

