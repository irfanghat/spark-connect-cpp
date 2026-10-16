#pragma once

#include "types/ArrayType.h"
#include "types/BinaryType.h"
#include "types/BooleanType.h"
#include "types/ByteType.h"
#include "types/CharType.h"
#include "types/DateType.h"
#include "types/DecimalType.h"
#include "types/DoubleType.h"
#include "types/FloatType.h"
#include "types/IntegerType.h"
#include "types/LongType.h"
#include "types/MapType.h"
#include "types/NullType.h"
#include "types/ShortType.h"
#include "types/StringType.h"
#include "types/StructType.h"
#include "types/TimestampNtzType.h"
#include "types/TimestampType.h"
#include "types/VarCharType.h"

#include <memory>
#include <string>
#include <utility>
#include <variant>

namespace spark::connect
{
class DataType;
} // namespace spark::connect

namespace spark::sql::types
{
using DataTypeVariant =
    std::variant<NullType, BooleanType, ByteType, ShortType, IntegerType,
                 LongType, FloatType, DoubleType, StringType, BinaryType,
                 DateType, TimestampType, TimestampNtzType, DecimalType,
                 CharType, VarCharType, ArrayType, MapType, StructType>;

class DataType
{
  public:
    DataTypeVariant kind;
    DataType(DataTypeVariant k) : kind(std::move(k))
    {
    }

    /**
     * @brief Returns the JSON representation of the type, compatible with
     * Spark's StructType.json().
     */
    std::string json() const;

    /**
     * @brief Returns a simple string name for the type (e.g., "integer",
     * "struct").
     */
    std::string type_name() const;

    /**
     * @brief Factory method to create a DataType from a Spark Connect
     * Protobuf message.
     */
    static DataType from_proto(spark::connect::DataType const& proto);
};
} // namespace spark::sql::types
