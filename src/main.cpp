#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "spark_types.h"

//----------------------------------------------------------------
// Include the protobuf generated header for direct manipulation
//-------------------------------------------------------------------
#include "spark/connect/types.pb.h"

using namespace spark::client;
using namespace spark::connect;

void print_data_type(const std::shared_ptr<SparkDataType> &dt)
{
    std::cout << "--- Data Type Info ---" << std::endl;
    if (!dt)
    {
        std::cout << "  (nullptr)" << std::endl;
        return;
    }

    std::cout << "  Type ID: ";
    switch (dt->GetTypeId())
    {
    case SparkDataType::TypeId::NULL_TYPE:
        std::cout << "NULL_TYPE";
        break;
    case SparkDataType::TypeId::BINARY:
        std::cout << "BINARY";
        break;
    case SparkDataType::TypeId::BOOLEAN:
        std::cout << "BOOLEAN";
        break;
    case SparkDataType::TypeId::BYTE:
        std::cout << "BYTE";
        break;
    case SparkDataType::TypeId::SHORT:
        std::cout << "SHORT";
        break;
    case SparkDataType::TypeId::INTEGER:
        std::cout << "INTEGER";
        break;
    case SparkDataType::TypeId::LONG:
        std::cout << "LONG";
        break;
    case SparkDataType::TypeId::FLOAT:
        std::cout << "FLOAT";
        break;
    case SparkDataType::TypeId::DOUBLE:
        std::cout << "DOUBLE";
        break;
    case SparkDataType::TypeId::DECIMAL:
        std::cout << "DECIMAL";
        break;
    case SparkDataType::TypeId::STRING:
        std::cout << "STRING";
        break;
    case SparkDataType::TypeId::CHAR:
        std::cout << "CHAR";
        break;
    case SparkDataType::TypeId::VARCHAR:
        std::cout << "VARCHAR";
        break;
    case SparkDataType::TypeId::DATE:
        std::cout << "DATE";
        break;
    case SparkDataType::TypeId::TIMESTAMP:
        std::cout << "TIMESTAMP";
        break;
    case SparkDataType::TypeId::TIMESTAMP_NTZ:
        std::cout << "TIMESTAMP_NTZ";
        break;
    case SparkDataType::TypeId::CALENDAR_INTERVAL:
        std::cout << "CALENDAR_INTERVAL";
        break;
    case SparkDataType::TypeId::YEAR_MONTH_INTERVAL:
        std::cout << "YEAR_MONTH_INTERVAL";
        break;
    case SparkDataType::TypeId::DAY_TIME_INTERVAL:
        std::cout << "DAY_TIME_INTERVAL";
        break;
    case SparkDataType::TypeId::ARRAY:
        std::cout << "ARRAY";
        break;
    case SparkDataType::TypeId::STRUCT:
        std::cout << "STRUCT";
        break;
    case SparkDataType::TypeId::MAP:
        std::cout << "MAP";
        break;
    case SparkDataType::TypeId::VARIANT:
        std::cout << "VARIANT";
        break;
    case SparkDataType::TypeId::UDT:
        std::cout << "UDT";
        break;
    case SparkDataType::TypeId::UNPARSED:
        std::cout << "UNPARSED";
        break;
    case SparkDataType::TypeId::UNKNOWN:
    default:
        std::cout << "UNKNOWN";
        break;
    }
    std::cout << std::endl;

    std::cout << "  Type Variation Ref: " << dt->type_variation_reference() << std::endl;

    if (dt->isString())
    {
        std::shared_ptr<StringType> str_dt = std::static_pointer_cast<StringType>(dt);
        std::cout << "  String Collation: " << str_dt->collation() << std::endl;
    }
    else if (dt->isChar())
    {
        std::shared_ptr<CharType> char_dt = std::static_pointer_cast<CharType>(dt);
        std::cout << "  Char Length: " << char_dt->length() << std::endl;
    }
    else if (dt->isDecimal())
    {
        std::shared_ptr<DecimalType> dec_dt = std::static_pointer_cast<DecimalType>(dt);
        std::cout << "  Decimal Precision: " << (dec_dt->precision().has_value() ? std::to_string(dec_dt->precision().value()) : "N/A") << std::endl;
        std::cout << "  Decimal Scale: " << (dec_dt->scale().has_value() ? std::to_string(dec_dt->scale().value()) : "N/A") << std::endl;
    }
    else if (dt->isStruct())
    {
        std::shared_ptr<StructType> struct_dt = std::static_pointer_cast<StructType>(dt);
        std::cout << "  Struct Fields:" << std::endl;
        for (const auto &field : struct_dt->fields())
        {
            std::cout << "    - Name: " << field.name()
                      << ", Nullable: " << (field.nullable() ? "true" : "false")
                      << ", Type ID: ";

            //------------------------------------
            // Access type ID of nested data type
            //------------------------------------
            if (field.data_type())
            {
                switch (field.data_type()->GetTypeId())
                {
                case SparkDataType::TypeId::INTEGER:
                    std::cout << "INTEGER";
                    break;
                case SparkDataType::TypeId::STRING:
                    std::cout << "STRING";
                    break;
                case SparkDataType::TypeId::BOOLEAN:
                    std::cout << "BOOLEAN";
                    break;
                default:
                    std::cout << "OTHER";
                    break;
                }
            }
            else
            {
                std::cout << "NULL";
            }
            std::cout << std::endl;
        }
    }
    else if (dt->isArray())
    {
        std::shared_ptr<ArrayType> array_dt = std::static_pointer_cast<ArrayType>(dt);
        std::cout << "  Array Contains Null: " << (array_dt->contains_null() ? "true" : "false") << std::endl;
        std::cout << "  Array Element Type ID: ";
        if (array_dt->element_type())
        {
            switch (array_dt->element_type()->GetTypeId())
            {
            case SparkDataType::TypeId::INTEGER:
                std::cout << "INTEGER";
                break;
            case SparkDataType::TypeId::STRING:
                std::cout << "STRING";
                break;
            default:
                std::cout << "OTHER";
                break;
            }
        }
        else
        {
            std::cout << "NULL";
        }
        std::cout << std::endl;
    }
    std::cout << "--------------------" << std::endl
              << std::endl;
}

int main()
{
    std::cout << "Testing SparkDataType wrappers..." << std::endl
              << std::endl;

    // 1. Create a simple IntegerType
    std::cout << "Creating IntegerType..." << std::endl;
    std::shared_ptr<IntegerType> int_type = std::make_shared<IntegerType>();
    print_data_type(int_type);

    // 2. Create a StringType with custom collation
    std::cout << "Creating StringType with collation..." << std::endl;
    std::shared_ptr<StringType> utf8_string = std::make_shared<StringType>(0, "utf8_general_ci");
    print_data_type(utf8_string);

    // 3. Create a CharType
    std::cout << "Creating CharType with length 10..." << std::endl;
    std::shared_ptr<CharType> char_type = std::make_shared<CharType>(10);
    print_data_type(char_type);

    // 4. Create a DecimalType with precision and scale
    std::cout << "Creating DecimalType(10, 2)..." << std::endl;
    std::shared_ptr<DecimalType> decimal_type = std::make_shared<DecimalType>(10, 2);
    print_data_type(decimal_type);

    // 5. Create a StructType (complex type)
    std::cout << "Creating StructType with nested types..." << std::endl;
    std::vector<StructField> fields;
    fields.emplace_back("id", std::make_shared<LongType>(), false);
    fields.emplace_back("name", std::make_shared<StringType>(0, "utf8_bin"), true);
    fields.emplace_back("is_active", std::make_shared<BooleanType>(), false);

    std::shared_ptr<StructType> complex_struct = std::make_shared<StructType>(fields);
    print_data_type(complex_struct);

    // 6. Create an ArrayType
    std::cout << "Creating ArrayType of Strings..." << std::endl;
    std::shared_ptr<ArrayType> string_array = std::make_shared<ArrayType>(std::make_shared<StringType>(), true);
    print_data_type(string_array);

    //-----------------------------------------------
    // Test Serialization and Deserialization
    //-----------------------------------------------
    std::cout << "--- Testing Serialization & Deserialization ---" << std::endl
              << std::endl;

    // A. Serialize the complex_struct
    DataType proto_complex_struct = complex_struct->ToProto();
    std::string serialized_data;
    if (proto_complex_struct.SerializeToString(&serialized_data))
    {
        std::cout << "Serialized complex_struct to string. Length: " << serialized_data.length() << " bytes." << std::endl;
    }
    else
    {
        std::cerr << "ERROR: Failed to serialize complex_struct!" << std::endl;
        return 1;
    }

    // B. Deserialize it back into a raw Protobuf message
    DataType deserialized_proto_struct;
    if (deserialized_proto_struct.ParseFromString(serialized_data))
    {
        std::cout << "Successfully deserialized raw Protobuf message." << std::endl;
        std::cout << "Deserialized Protobuf Kind: ";
        switch (deserialized_proto_struct.kind_case())
        {
        case DataType::kStruct:
            std::cout << "STRUCT";
            break;
        default:
            std::cout << "OTHER (" << deserialized_proto_struct.kind_case() << ")";
            break;
        }
        std::cout << std::endl;
    }
    else
    {
        std::cerr << "ERROR: Failed to parse raw Protobuf message from string!" << std::endl;
        return 1;
    }

    // C. Use SparkDataType::FromProto to convert the deserialized Protobuf message
    std::shared_ptr<SparkDataType> rehydrated_struct = SparkDataType::FromProto(deserialized_proto_struct);
    std::cout << "Rehydrated StructType from deserialized Protobuf:" << std::endl;
    print_data_type(rehydrated_struct);

    if (rehydrated_struct->isStruct())
    {
        std::shared_ptr<StructType> cast_rehydrated_struct = std::static_pointer_cast<StructType>(rehydrated_struct);
        if (cast_rehydrated_struct->fields().size() == 3)
        {
            std::cout << "  SUCCESS: Rehydrated struct has correct number of fields (3)." << std::endl;
            std::cout << "  Field 1 Name: " << cast_rehydrated_struct->fields()[0].name() << std::endl;
            std::cout << "  Field 2 Name: " << cast_rehydrated_struct->fields()[1].name() << std::endl;
            std::cout << "  Field 3 Type ID: " << (cast_rehydrated_struct->fields()[2].data_type() && cast_rehydrated_struct->fields()[2].data_type()->isBoolean() ? "BOOLEAN" : "ERROR") << std::endl;
        }
        else
        {
            std::cerr << "  ERROR: Rehydrated struct has incorrect number of fields." << std::endl;
        }
    }
    else
    {
        std::cerr << "  ERROR: Rehydrated type is not a StructType!" << std::endl;
    }

    std::cout << "\nAll tests completed." << std::endl;

    return 0;
}