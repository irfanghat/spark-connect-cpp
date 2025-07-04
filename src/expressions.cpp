/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "spark/connect/expressions.pb.h"
#include "spark/connect/types.pb.h" // For spark::connect::DataType
#include "expressions.h"
#include "types.h"   // For SparkDataType::FromProto (assuming this is your custom wrapper)
#include <utility>   // For std::move
#include <stdexcept> // For std::runtime_error
#include <string>    // For std::string conversion

namespace spark
{
    namespace client
    {

        //------------------------------------------------------------------------------------------------------
        // SparkExpression Base Class Implementation
        // This provides the concrete implementation for the base class, making it compilable and testable.
        //------------------------------------------------------------------------------------------------------

        //------------------------------------------------------------------------------------------------------
        // Constructor: Initializes the internal Protobuf message with the provided one.
        //------------------------------------------------------------------------------------------------------
        SparkExpression::SparkExpression(const spark::connect::Expression &proto) : proto_(proto) {}

        //------------------------------------------------------------------------------------------------------
        // Static FromProto method: Factory function to create specific SparkExpression
        // derived classes based on the type of expression present in the Protobuf message.
        //------------------------------------------------------------------------------------------------------
        std::shared_ptr<SparkExpression> SparkExpression::FromProto(const spark::connect::Expression &proto)
        {
            if (proto.has_literal())
            {
                return std::make_shared<LiteralExpression>(proto);
            }
            else if (proto.has_unresolved_attribute())
            {
                return std::make_shared<UnresolvedAttributeExpression>(proto);
            }
            else if (proto.has_unresolved_function())
            {
                return std::make_shared<UnresolvedFunctionExpression>(proto);
            }
            else if (proto.has_unresolved_star())
            {
                return std::make_shared<UnresolvedStarExpression>(proto);
            }
            else if (proto.has_alias())
            {
                return std::make_shared<AliasExpression>(proto);
            }
            else if (proto.has_cast())
            {
                return std::make_shared<CastExpression>(proto);
            }

            //------------------------------------------------------------------------------------------------------
            // To do: `else if` conditions here for other expression types as we implement them.
            // Example for placeholder in the .proto:
            // else if (proto.has_expression_type_X()) {
            //     return std::make_shared<ExpressionTypeX>(proto);
            // }
            //------------------------------------------------------------------------------------------------------

            //------------------------------------------------------------------------------------------------------
            // If none of the known expression types match, throw an error.
            //------------------------------------------------------------------------------------------------------
            throw std::runtime_error("SparkExpression::FromProto: Unknown expression type from Protobuf.");
        }

        //------------------------------------------------------------------------------------------------------
        // LiteralExpression Implementation
        //------------------------------------------------------------------------------------------------------
        LiteralExpression::LiteralExpression(bool value)
        {
            proto_.mutable_literal()->set_boolean(value);
            value_ = value;
        }
        LiteralExpression::LiteralExpression(int32_t value)
        {
            proto_.mutable_literal()->set_integer(value);
            value_ = value;
        }
        LiteralExpression::LiteralExpression(int64_t value)
        {
            proto_.mutable_literal()->set_long_(value); // 'long_' to avoid C++ keyword conflict
            value_ = value;
        }
        LiteralExpression::LiteralExpression(float value)
        {
            proto_.mutable_literal()->set_float_(value); // 'float_' to avoid C++ keyword conflict
            value_ = value;
        }
        LiteralExpression::LiteralExpression(double value)
        {
            proto_.mutable_literal()->set_double_(value); // 'double_' to avoid C++ keyword conflict
            value_ = value;
        }
        LiteralExpression::LiteralExpression(const std::string &value)
        {
            proto_.mutable_literal()->set_string(value);
            value_ = value;
        }
        LiteralExpression::LiteralExpression(std::string &&value)
        {
            proto_.mutable_literal()->set_string(std::move(value));
            //------------------------------------------------------------------------------------------------------
            // A safer way to populate value_ is often done in a separate `setValueFromProto` or by re-reading.
            // For now, we assume this is fine or remove it if not strictly needed immediately after set.
            //------------------------------------------------------------------------------------------------------
            value_ = proto_.literal().string(); // Copy the set value from proto to variant
        }
        LiteralExpression::LiteralExpression(const std::vector<uint8_t> &value)
        {
            proto_.mutable_literal()->set_binary(value.data(), value.size());
            value_ = value;
        }
        LiteralExpression::LiteralExpression(std::vector<uint8_t> &&value)
        {
            proto_.mutable_literal()->set_binary(value.data(), value.size());
            value_ = std::move(value); // Move for the variant storage
        }
        LiteralExpression::LiteralExpression(std::nullptr_t)
        {
            proto_.mutable_literal()->mutable_null();
            value_ = std::monostate(); // Explicitly set to monostate for null
        }

        LiteralExpression::LiteralExpression(const spark::connect::Expression &proto)
            : SparkExpression(proto)
        {
            if (!proto_.has_literal())
            {
                throw std::runtime_error("Protobuf Expression does not contain a Literal field.");
            }
            setValueFromProto();
        }

        void LiteralExpression::setValueFromProto()
        {
            const auto &literal_proto = proto_.literal();
            switch (literal_proto.literal_type_case())
            {
            case spark::connect::Expression_Literal::kBoolean:
                value_ = literal_proto.boolean();
                break;
            case spark::connect::Expression_Literal::kByte:
                value_ = literal_proto.byte();
                break;
            case spark::connect::Expression_Literal::kShort:
                value_ = literal_proto.short_();
                break;
            case spark::connect::Expression_Literal::kInteger:
                value_ = literal_proto.integer();
                break;
            case spark::connect::Expression_Literal::kLong:
                value_ = literal_proto.long_();
                break;
            case spark::connect::Expression_Literal::kFloat:
                value_ = literal_proto.float_();
                break;
            case spark::connect::Expression_Literal::kDouble:
                value_ = literal_proto.double_();
                break;
            case spark::connect::Expression_Literal::kString:
                value_ = literal_proto.string();
                break;
            case spark::connect::Expression_Literal::kBinary:
                value_ = std::vector<uint8_t>(literal_proto.binary().begin(), literal_proto.binary().end());
                break;
            case spark::connect::Expression_Literal::kNull:
                value_ = std::monostate();
                //------------------------------------------------------------------------------------------------------
                // TODO: If the null has a specific DataType, we might want to store it as part of value_
                // For now, we only know it's a null.
                //------------------------------------------------------------------------------------------------------
                break;
            case spark::connect::Expression_Literal::kDate:
                value_ = literal_proto.date();
                break;
            case spark::connect::Expression_Literal::kTimestamp:
                value_ = literal_proto.timestamp();
                break;
            case spark::connect::Expression_Literal::kTimestampNtz:
                value_ = literal_proto.timestamp_ntz();
                break;
            case spark::connect::Expression_Literal::kYearMonthInterval:
                value_ = literal_proto.year_month_interval();
                break;
            case spark::connect::Expression_Literal::kDayTimeInterval:
                value_ = literal_proto.day_time_interval();
                break;
            //------------------------------------------------------------------------------------------------------
            // TODO: Handle other literal types (Decimal, CalendarInterval, Array, Map, Struct etc.)
            // These would require adding corresponding types to LiteralExpression::ValueType
            //------------------------------------------------------------------------------------------------------
            case spark::connect::Expression_Literal::kDecimal:
            case spark::connect::Expression_Literal::kCalendarInterval:
            case spark::connect::Expression_Literal::kArray:
            case spark::connect::Expression_Literal::kMap:
            case spark::connect::Expression_Literal::kStruct:
                throw std::runtime_error("Literal type not yet supported in C++ client: " + std::to_string(literal_proto.literal_type_case()));
            case spark::connect::Expression_Literal::LITERAL_TYPE_NOT_SET:
            default:
                throw std::runtime_error("Unknown or unset Literal type in Protobuf message: " + std::to_string(literal_proto.literal_type_case()));
            }
        }


        //------------------------------------------------------------------------------------------------------
        // UnresolvedAttributeExpression Implementation
        //------------------------------------------------------------------------------------------------------
        UnresolvedAttributeExpression::UnresolvedAttributeExpression(
            std::string unparsed_identifier,
            std::optional<int64_t> plan_id,
            std::optional<bool> is_metadata_column)
        {
            auto attr_proto = proto_.mutable_unresolved_attribute();
            attr_proto->set_unparsed_identifier(std::move(unparsed_identifier));
            if (plan_id.has_value())
            {
                attr_proto->set_plan_id(plan_id.value());
            }
            if (is_metadata_column.has_value())
            {
                attr_proto->set_is_metadata_column(is_metadata_column.value());
            }
        }

        UnresolvedAttributeExpression::UnresolvedAttributeExpression(const spark::connect::Expression &proto)
            : SparkExpression(proto)
        {
            if (!proto_.has_unresolved_attribute())
            {
                throw std::runtime_error("Protobuf Expression does not contain an UnresolvedAttribute field.");
            }
        }

        //------------------------------------------------------------------------------------------------------
        // UnresolvedFunctionExpression Implementation
        //------------------------------------------------------------------------------------------------------
        UnresolvedFunctionExpression::UnresolvedFunctionExpression(
            std::string function_name,
            std::vector<std::shared_ptr<SparkExpression>> arguments,
            bool is_distinct,
            bool is_user_defined_function)
            : arguments_(std::move(arguments))
        {
            auto func_proto = proto_.mutable_unresolved_function();
            func_proto->set_function_name(std::move(function_name));
            func_proto->set_is_distinct(is_distinct);
            func_proto->set_is_user_defined_function(is_user_defined_function);
            for (const auto &arg : arguments_)
            {
                if (arg)
                {
                    *func_proto->add_arguments() = arg->ToProto();
                }
                else
                {
                    throw std::runtime_error("Attempted to add a null SparkExpression argument to UnresolvedFunctionExpression.");
                }
            }
        }

        UnresolvedFunctionExpression::UnresolvedFunctionExpression(const spark::connect::Expression &proto)
            : SparkExpression(proto)
        {
            if (!proto_.has_unresolved_function())
            {
                throw std::runtime_error("Protobuf Expression does not contain an UnresolvedFunction field.");
            }
            for (const auto &arg_proto : proto_.unresolved_function().arguments())
            {
                arguments_.push_back(SparkExpression::FromProto(arg_proto));
            }
        }

        //------------------------------------------------------------------------------------------------------
        // UnresolvedStarExpression Implementation
        //------------------------------------------------------------------------------------------------------
        UnresolvedStarExpression::UnresolvedStarExpression(std::optional<std::string> unparsed_target,
                                                           std::optional<int64_t> plan_id)
        {
            auto star_proto = proto_.mutable_unresolved_star();
            if (unparsed_target.has_value())
            {
                star_proto->set_unparsed_target(std::move(unparsed_target.value()));
            }
            if (plan_id.has_value())
            {
                star_proto->set_plan_id(plan_id.value());
            }
        }

        UnresolvedStarExpression::UnresolvedStarExpression(const spark::connect::Expression &proto)
            : SparkExpression(proto)
        {
            if (!proto_.has_unresolved_star())
            {
                throw std::runtime_error("Protobuf Expression does not contain an UnresolvedStar field.");
            }
        }

        //------------------------------------------------------------------------------------------------------
        // AliasExpression Implementation
        //------------------------------------------------------------------------------------------------------
        AliasExpression::AliasExpression(std::shared_ptr<SparkExpression> expr,
                                         std::vector<std::string> name_parts,
                                         std::optional<std::string> metadata)
            : expr_(std::move(expr))
        {
            if (!expr_)
            {
                throw std::runtime_error("AliasExpression requires a non-null expression to alias.");
            }
            auto alias_proto = proto_.mutable_alias();
            *alias_proto->mutable_expr() = expr_->ToProto();
            for (auto &n_part : name_parts)
            {
                alias_proto->add_name(std::move(n_part));
            }
            if (metadata.has_value())
            {
                alias_proto->set_metadata(std::move(metadata.value()));
            }
        }

        AliasExpression::AliasExpression(const spark::connect::Expression &proto)
            : SparkExpression(proto)
        {
            if (!proto_.has_alias())
            {
                throw std::runtime_error("Protobuf Expression does not contain an Alias field.");
            }
            expr_ = SparkExpression::FromProto(proto_.alias().expr());
        }

        //------------------------------------------------------------------------------------------------------
        // CastExpression Implementation
        //------------------------------------------------------------------------------------------------------

        //------------------------------------------------------------------------------------------------------
        // Helper to convert proto EvalMode to C++ CastEvalMode
        //------------------------------------------------------------------------------------------------------
        static CastEvalMode toCastEvalMode(spark::connect::Expression_Cast_EvalMode proto_eval_mode)
        {
            switch (proto_eval_mode)
            {
            case spark::connect::Expression_Cast_EvalMode_EVAL_MODE_UNSPECIFIED:
                return CastEvalMode::UNSPECIFIED;
            case spark::connect::Expression_Cast_EvalMode_EVAL_MODE_LEGACY:
                return CastEvalMode::LEGACY;
            case spark::connect::Expression_Cast_EvalMode_EVAL_MODE_ANSI:
                return CastEvalMode::ANSI;
            case spark::connect::Expression_Cast_EvalMode_EVAL_MODE_TRY:
                return CastEvalMode::TRY;
            default:
                throw std::runtime_error("Unknown Cast EvalMode received from Protobuf.");
            }
        }

        //------------------------------------------------------------------------------------------------------
        // Helper to convert C++ CastEvalMode to proto EvalMode
        //------------------------------------------------------------------------------------------------------
        static spark::connect::Expression_Cast_EvalMode toProtoEvalMode(CastEvalMode cpp_eval_mode)
        {
            switch (cpp_eval_mode)
            {
            case CastEvalMode::UNSPECIFIED:
                return spark::connect::Expression_Cast_EvalMode_EVAL_MODE_UNSPECIFIED;
            case CastEvalMode::LEGACY:
                return spark::connect::Expression_Cast_EvalMode_EVAL_MODE_LEGACY;
            case CastEvalMode::ANSI:
                return spark::connect::Expression_Cast_EvalMode_EVAL_MODE_ANSI;
            case CastEvalMode::TRY:
                return spark::connect::Expression_Cast_EvalMode_EVAL_MODE_TRY;
            default:
                throw std::runtime_error("Unknown CastEvalMode provided.");
            }
        }

        CastExpression::CastExpression(std::shared_ptr<SparkExpression> expr,
                                       std::shared_ptr<SparkDataType> data_type,
                                       CastEvalMode eval_mode)
            : expr_(std::move(expr)), data_type_(std::move(data_type))
        {
            if (!expr_)
            {
                throw std::runtime_error("CastExpression requires a non-null expression to cast.");
            }
            if (!data_type_)
            {
                throw std::runtime_error("CastExpression requires a non-null data type to cast to.");
            }
            auto cast_proto = proto_.mutable_cast();
            *cast_proto->mutable_expr() = expr_->ToProto();
            *cast_proto->mutable_type() = data_type_->ToProto();   // Set the 'type' field in the oneof
            cast_proto->set_eval_mode(toProtoEvalMode(eval_mode)); // Set the enum field
        }

        CastExpression::CastExpression(const spark::connect::Expression &proto)
            : SparkExpression(proto)
        {
            if (!proto_.has_cast())
            {
                throw std::runtime_error("Protobuf Expression does not contain a Cast field.");
            }
            expr_ = SparkExpression::FromProto(proto_.cast().expr());

            // Handle the 'cast_to_type' oneof
            switch (proto_.cast().cast_to_type_case())
            {
            case spark::connect::Expression_Cast::kType:
                data_type_ = SparkDataType::FromProto(proto_.cast().type());
                break;
            case spark::connect::Expression_Cast::kTypeStr:
                throw std::runtime_error("CastExpression: Cannot construct from proto with 'type_str'. This C++ wrapper currently only supports 'type'.");
                break; // Not reachable due to throw
            case spark::connect::Expression_Cast::CAST_TO_TYPE_NOT_SET:
            default:
                throw std::runtime_error("CastExpression: Protobuf Cast message must have either 'type' or 'type_str' set in oneof.");
            }
        }

        CastEvalMode CastExpression::eval_mode() const
        {
            return toCastEvalMode(proto_.cast().eval_mode());
        }

        //------------------------------------------------------------------------------------------------------
        // Global Convenience Functions
        //------------------------------------------------------------------------------------------------------
        std::shared_ptr<UnresolvedAttributeExpression> col(const std::string &unparsed_identifier,
                                                           std::optional<int64_t> plan_id,
                                                           std::optional<bool> is_metadata_column)
        {
            return std::make_shared<UnresolvedAttributeExpression>(unparsed_identifier, plan_id, is_metadata_column);
        }

        std::shared_ptr<UnresolvedFunctionExpression> call_function(
            const std::string &name,
            std::vector<std::shared_ptr<SparkExpression>> args,
            bool is_distinct,
            bool is_user_defined_function)
        {
            return std::make_shared<UnresolvedFunctionExpression>(name, std::move(args), is_distinct, is_user_defined_function);
        }

        std::shared_ptr<UnresolvedStarExpression> star(std::optional<std::string> unparsed_target,
                                                       std::optional<int64_t> plan_id)
        {
            return std::make_shared<UnresolvedStarExpression>(unparsed_target, plan_id);
        }

        std::shared_ptr<AliasExpression> alias(std::shared_ptr<SparkExpression> expr,
                                               std::vector<std::string> name_parts,
                                               std::optional<std::string> metadata)
        {
            return std::make_shared<AliasExpression>(std::move(expr), std::move(name_parts), std::move(metadata));
        }

        std::shared_ptr<CastExpression> cast(std::shared_ptr<SparkExpression> expr,
                                             std::shared_ptr<SparkDataType> data_type,
                                             CastEvalMode eval_mode)
        {
            return std::make_shared<CastExpression>(std::move(expr), std::move(data_type), eval_mode);
        }

    } // namespace client
} // namespace spark