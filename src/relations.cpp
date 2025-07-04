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

#include "relations.h"
#include "expressions.h" // Need full definition for SparkExpression::FromProto placeholder
#include "types.h" // For SparkDataType::FromProto if needed (e.g. for ToSchema relation)
#include <utility>       // For std::move

namespace spark
{
    namespace client
    {

        // Dummy implementation for SparkExpression::ToProto to allow compilation
        const spark::connect::Expression &SparkExpression::ToProto() const
        {
            static spark::connect::Expression dummy_expr;
            return dummy_expr; // Return an empty expression
        }
        // Dummy implementation for SparkExpression::FromProto
        std::shared_ptr<SparkExpression> SparkExpression::FromProto(const spark::connect::Expression &proto)
        {
            // In a real implementation, this would parse the expression oneof
            // For now, return a nullptr or throw an error.
            throw std::runtime_error("SparkExpression::FromProto not implemented yet. Expression proto kind: " + std::to_string(proto.expr_type_case()));
            return nullptr;
        }

        // --- SparkRelation Base Class Implementation ---

        std::shared_ptr<SparkRelation> SparkRelation::FromProto(const spark::connect::Relation &proto)
        {
            switch (proto.rel_type_case())
            {
            case spark::connect::Relation::kRead:
                return std::make_shared<ReadRelation>(proto);
            case spark::connect::Relation::kProject:
                return std::make_shared<ProjectRelation>(proto);
            case spark::connect::Relation::kFilter:
                return std::make_shared<FilterRelation>(proto);
            case spark::connect::Relation::kLocalRelation:
                return std::make_shared<LocalRelation>(proto);
            case spark::connect::Relation::kRange: // <--- RangeRelation factory creation
                return std::make_shared<RangeRelation>(proto);
            // TODO: Add more cases as relations are implemented
            case spark::connect::Relation::REL_TYPE_NOT_SET:
            default:
                throw std::runtime_error("Unknown or unset Spark Relation kind in Protobuf message: " + std::to_string(proto.rel_type_case()));
            }
        }

        // --- ReadRelation Implementation ---

        ReadRelation::NamedTable::NamedTable(std::string unparsed_identifier, std::map<std::string, std::string> options)
        {
            proto_.set_unparsed_identifier(std::move(unparsed_identifier));
            // Directly use mutable_options() which returns google::protobuf::Map*
            auto mutable_map = proto_.mutable_options();
            for (const auto &pair : options)
            {
                (*mutable_map)[pair.first] = pair.second;
            }
        }

        ReadRelation::DataSource::DataSource(std::optional<std::string> format,
                                             std::optional<std::string> schema,
                                             std::map<std::string, std::string> options,
                                             std::vector<std::string> paths,
                                             std::vector<std::string> predicates)
        {
            if (format.has_value())
            {
                proto_.set_format(std::move(format.value()));
            }
            if (schema.has_value())
            {
                proto_.set_schema(std::move(schema.value()));
            }
            // Directly use mutable_options()
            auto mutable_options = proto_.mutable_options();
            for (const auto &pair : options)
            {
                (*mutable_options)[pair.first] = pair.second;
            }
            // Directly use add_paths()
            for (const auto &path : paths)
            {
                proto_.add_paths(path);
            }
            // Directly use add_predicates()
            for (const auto &predicate : predicates)
            {
                proto_.add_predicates(predicate);
            }
        }

        ReadRelation::ReadRelation(NamedTable named_table, bool is_streaming)
        {
            auto read_proto = proto_.mutable_read();
            *read_proto->mutable_named_table() = named_table.ToProto();
            read_proto->set_is_streaming(is_streaming);
        }

        ReadRelation::ReadRelation(DataSource data_source, bool is_streaming)
        {
            auto read_proto = proto_.mutable_read();
            *read_proto->mutable_data_source() = data_source.ToProto();
            read_proto->set_is_streaming(is_streaming);
        }

        ReadRelation::NamedTable ReadRelation::named_table() const
        {
            if (!has_named_table())
            {
                throw std::runtime_error("Relation is not a NamedTable read.");
            }
            return NamedTable(proto_.read().named_table());
        }

        ReadRelation::DataSource ReadRelation::data_source() const
        {
            if (!has_data_source())
            {
                throw std::runtime_error("Relation is not a DataSource read.");
            }
            return DataSource(proto_.read().data_source());
        }

        // --- ProjectRelation Implementation ---

        ProjectRelation::ProjectRelation(std::vector<std::shared_ptr<SparkExpression>> expressions,
                                         std::shared_ptr<SparkRelation> input)
            : expressions_(std::move(expressions)), input_(std::move(input))
        {
            auto project_proto = proto_.mutable_project();
            if (input_)
            {
                *project_proto->mutable_input() = input_->ToProto();
            }
            for (const auto &expr : expressions_)
            {
                // Ensure expr is not null before calling ToProto()
                if (expr)
                {
                    *project_proto->add_expressions() = expr->ToProto();
                }
                else
                {
                    // Handle null expression, e.g., by skipping or throwing an error
                    throw std::runtime_error("Attempted to add a null SparkExpression to ProjectRelation.");
                }
            }
        }

        ProjectRelation::ProjectRelation(const spark::connect::Relation &proto)
            : SparkRelation(proto)
        {
            // Deserialize input
            if (proto_.project().has_input())
            {
                input_ = SparkRelation::FromProto(proto_.project().input());
            }
            // Deserialize expressions
            for (const auto &expr_proto : proto_.project().expressions())
            {
                expressions_.push_back(SparkExpression::FromProto(expr_proto));
            }
        }

        // --- FilterRelation Implementation ---

        FilterRelation::FilterRelation(std::shared_ptr<SparkRelation> input,
                                       std::shared_ptr<SparkExpression> condition)
            : input_(std::move(input)), condition_(std::move(condition))
        {
            auto filter_proto = proto_.mutable_filter();
            if (!input_)
            {
                throw std::runtime_error("FilterRelation requires a non-null input relation.");
            }
            if (!condition_)
            {
                throw std::runtime_error("FilterRelation requires a non-null condition expression.");
            }
            *filter_proto->mutable_input() = input_->ToProto();
            *filter_proto->mutable_condition() = condition_->ToProto();
        }

        FilterRelation::FilterRelation(const spark::connect::Relation &proto)
            : SparkRelation(proto)
        {
            // Deserialize input
            input_ = SparkRelation::FromProto(proto_.filter().input());
            // Deserialize condition
            condition_ = SparkExpression::FromProto(proto_.filter().condition());
        }

        // --- LocalRelation Implementation ---

        LocalRelation::LocalRelation(std::string schema_str)
        {
            auto local_proto = proto_.mutable_local_relation();
            local_proto->set_schema(std::move(schema_str));
            // No data set, so has_data() will be false
        }

        LocalRelation::LocalRelation(std::optional<std::string> data_base64_encoded,
                                     std::optional<std::string> schema)
        {
            auto local_proto = proto_.mutable_local_relation();
            if (data_base64_encoded.has_value())
            {
                local_proto->set_data(data_base64_encoded.value()); // Protobuf expects raw bytes here
            }
            if (schema.has_value())
            {
                local_proto->set_schema(std::move(schema.value()));
            }
        }

        LocalRelation::LocalRelation(const spark::connect::Relation &proto)
            : SparkRelation(proto)
        {
            // Data and schema are directly accessible via proto_.local_relation()
        }

        // --- RangeRelation Implementation ---
        RangeRelation::RangeRelation(int64_t end,
                                     std::optional<int64_t> start,
                                     std::optional<int64_t> step,
                                     std::optional<int32_t> num_partitions)
        {
            auto range_proto = proto_.mutable_range();
            range_proto->set_end(end);
            // For optional fields, only set if value is present
            if (start.has_value())
            {
                range_proto->set_start(start.value());
            }
            if (step.has_value())
            {
                range_proto->set_step(step.value());
            }
            if (num_partitions.has_value())
            {
                range_proto->set_num_partitions(num_partitions.value());
            }
        }

        // --- Global Convenience Functions ---

        std::shared_ptr<ReadRelation> ReadTable(const std::string &unparsed_identifier,
                                                const std::map<std::string, std::string> &options,
                                                bool is_streaming)
        {
            return std::make_shared<ReadRelation>(
                ReadRelation::NamedTable(unparsed_identifier, options),
                is_streaming);
        }

        std::shared_ptr<ReadRelation> ReadDataSource(const std::string &format,
                                                     const std::map<std::string, std::string> &options,
                                                     const std::vector<std::string> &paths,
                                                     std::optional<std::string> schema,
                                                     const std::vector<std::string> &predicates,
                                                     bool is_streaming)
        {
            return std::make_shared<ReadRelation>(
                ReadRelation::DataSource(
                    format,
                    schema,
                    options,
                    paths,
                    predicates),
                is_streaming);
        }

        std::shared_ptr<RangeRelation> Range(int64_t end,
                                             std::optional<int64_t> start,
                                             std::optional<int64_t> step,
                                             std::optional<int32_t> num_partitions)
        {
            return std::make_shared<RangeRelation>(end, start, step, num_partitions);
        }

    } // namespace client
} // namespace spark