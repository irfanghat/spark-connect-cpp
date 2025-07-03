#ifndef SPARK_CONNECT_CPP_RELATIONS_H
#define SPARK_CONNECT_CPP_RELATIONS_H

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <optional>
#include <stdexcept>

#include "spark/connect/relations.pb.h"
#include "spark/connect/expressions.pb.h"
#include "spark/connect/common.pb.h"

// Include Protobuf container types
#include <google/protobuf/map.h>
#include <google/protobuf/repeated_field.h>

#include "expressions.h"
#include "spark_types.h"

namespace spark
{
    namespace client
    {
        //--------------------------------------------------------------
        // Forward declarations for all concrete SparkRelation types
        //--------------------------------------------------------------
        class SparkRelation;
        class ReadRelation;
        class ProjectRelation;
        class FilterRelation;
        class LocalRelation;
        class RangeRelation;
        // ... more relations will go here

        /**
         * @brief Base abstract class for all Spark Relation types.
         * This class directly holds the spark::connect::Relation Protobuf message.
         */
        class SparkRelation
        {
        public:
            enum class TypeId
            {
                UNKNOWN,
                READ,
                PROJECT,
                FILTER,
                LOCAL_RELATION,
                RANGE,
                // ... more relation types
            };

            //-------------------------------------------------------------------------
            // Constructor to initialize from a Protobuf message (used by FromProto)
            //-------------------------------------------------------------------------
            explicit SparkRelation(const spark::connect::Relation &proto) : proto_(proto) {}

            //-------------------------------------------------------------------------
            // Default constructor for derived classes
            //-------------------------------------------------------------------------
            SparkRelation() = default;
            virtual ~SparkRelation() = default;

            //-------------------------------------------------------------------------
            // Factory method to create a SparkRelation from a Protobuf Relation
            //-------------------------------------------------------------------------
            static std::shared_ptr<SparkRelation> FromProto(const spark::connect::Relation &proto);

            //-------------------------------------------------------------------------
            // Get the underlying Protobuf message
            //-------------------------------------------------------------------------
            const spark::connect::Relation &ToProto() const { return proto_; }
            spark::connect::Relation *MutableProto() { return &proto_; } // For internal use by derived classes

            //-------------------------------------------------------------------------
            // Pure virtual method to get the specific TypeId
            //-------------------------------------------------------------------------
            virtual TypeId GetTypeId() const = 0;

            //-------------------------------------------------------------------------
            // Access common metadata for the relation
            //-------------------------------------------------------------------------
            const spark::connect::RelationCommon &common() const { return proto_.common(); }
            spark::connect::RelationCommon *mutable_common() { return proto_.mutable_common(); }

        protected:
            spark::connect::Relation proto_; // The underlying Protobuf message
        };

        //-------------------------------------------------------------------------
        // Concrete Relation Implementations
        //-------------------------------------------------------------------------

        /**
         * @brief Represents a Spark Read relation.
         */
        class ReadRelation : public SparkRelation
        {
        public:
            TypeId GetTypeId() const override { return TypeId::READ; }
            //-------------------------------------------------------------------------
            // Nested classes for NamedTable and DataSource to mirror Protobuf structure
            //-------------------------------------------------------------------------
            class NamedTable
            {
            public:
                NamedTable(std::string unparsed_identifier, std::map<std::string, std::string> options = {});

                //-------------------------------------------------------------------------
                // Construct from Protobuf message
                //-------------------------------------------------------------------------
                explicit NamedTable(const spark::connect::Read::NamedTable &proto) : proto_(proto) {}

                const std::string &unparsed_identifier() const { return proto_.unparsed_identifier(); }

                //-------------------------------------------------------------------------
                // Return google::protobuf::Map directly
                //-------------------------------------------------------------------------
                const google::protobuf::Map<std::string, std::string> &options() const { return proto_.options(); }
                const spark::connect::Read::NamedTable &ToProto() const { return proto_; }

            private:
                spark::connect::Read::NamedTable proto_;
            };

            class DataSource
            {
            public:
                DataSource(std::optional<std::string> format = std::nullopt,
                           std::optional<std::string> schema = std::nullopt,
                           std::map<std::string, std::string> options = {}, // Input can still be std::map
                           std::vector<std::string> paths = {},
                           std::vector<std::string> predicates = {});

                //-------------------------------------------------------------------------
                // Construct from Protobuf message
                //-------------------------------------------------------------------------
                explicit DataSource(const spark::connect::Read::DataSource &proto) : proto_(proto) {}

                std::optional<std::string> format() const
                {
                    return proto_.has_format() ? std::make_optional(proto_.format()) : std::nullopt;
                }
                std::optional<std::string> schema() const
                {
                    return proto_.has_schema() ? std::make_optional(proto_.schema()) : std::nullopt;
                }

                //-------------------------------------------------------------------------
                // Return google::protobuf::Map directly
                //-------------------------------------------------------------------------
                const google::protobuf::Map<std::string, std::string> &options() const { return proto_.options(); }

                //-------------------------------------------------------------------------
                // Return google::protobuf::RepeatedPtrField directly
                //-------------------------------------------------------------------------
                const google::protobuf::RepeatedPtrField<std::string> &paths() const { return proto_.paths(); }

                //-------------------------------------------------------------------------
                // Return google::protobuf::RepeatedPtrField directly
                //-------------------------------------------------------------------------
                const google::protobuf::RepeatedPtrField<std::string> &predicates() const { return proto_.predicates(); }
                const spark::connect::Read::DataSource &ToProto() const { return proto_; }

            private:
                spark::connect::Read::DataSource proto_;
            };

            //-------------------------------------------------------------------------
            // Constructors for ReadRelation
            //-------------------------------------------------------------------------
            
            // For NamedTable reads
            ReadRelation(NamedTable named_table, bool is_streaming = false);
            // For DataSource reads
            ReadRelation(DataSource data_source, bool is_streaming = false);
            // For FromProto factory
            explicit ReadRelation(const spark::connect::Relation &proto) : SparkRelation(proto) {}

            //-------------------------------------------------------------------------
            // Accessors
            //-------------------------------------------------------------------------
            bool is_streaming() const { return proto_.read().is_streaming(); }
            bool has_named_table() const { return proto_.read().has_named_table(); }
            NamedTable named_table() const; // Returns a wrapper object
            bool has_data_source() const { return proto_.read().has_data_source(); }
            DataSource data_source() const; // Returns a wrapper object
        };

        /**
         * @brief Represents a Spark Project relation (select).
         */
        class ProjectRelation : public SparkRelation
        {
        public:
            TypeId GetTypeId() const override { return TypeId::PROJECT; }

            ProjectRelation(std::vector<std::shared_ptr<SparkExpression>> expressions,
                            std::shared_ptr<SparkRelation> input = nullptr); // input is optional in proto
            
            //-------------------------------------------------------------------------
            // For FromProto factory
            //-------------------------------------------------------------------------
            explicit ProjectRelation(const spark::connect::Relation &proto);

            const std::vector<std::shared_ptr<SparkExpression>> &expressions() const { return expressions_; }
            const std::shared_ptr<SparkRelation> &input() const { return input_; }

        private:
            std::vector<std::shared_ptr<SparkExpression>> expressions_;
            std::shared_ptr<SparkRelation> input_;
        };

        /**
         * @brief Represents a Spark Filter relation (where).
         */
        class FilterRelation : public SparkRelation
        {
        public:
            TypeId GetTypeId() const override { return TypeId::FILTER; }

            FilterRelation(std::shared_ptr<SparkRelation> input,
                           std::shared_ptr<SparkExpression> condition);
            
            //-------------------------------------------------------------------------
            // For FromProto factory
            //-------------------------------------------------------------------------
            explicit FilterRelation(const spark::connect::Relation &proto);

            const std::shared_ptr<SparkRelation> &input() const { return input_; }
            const std::shared_ptr<SparkExpression> &condition() const { return condition_; }

        private:
            std::shared_ptr<SparkRelation> input_;
            std::shared_ptr<SparkExpression> condition_;
        };

        /**
         * @brief Represents a Spark LocalRelation.
         */
        class LocalRelation : public SparkRelation
        {
        public:
            TypeId GetTypeId() const override { return TypeId::LOCAL_RELATION; }

            //-------------------------------------------------------------------------
            // Constructor for schema-only local relation
            //-------------------------------------------------------------------------
            LocalRelation(std::string schema);

            //-------------------------------------------------------------------------
            // Constructor for local relation with data (e.g., Arrow serialized data)
            //-------------------------------------------------------------------------
            LocalRelation(std::optional<std::string> data_base64_encoded,
                          std::optional<std::string> schema = std::nullopt);

            //-------------------------------------------------------------------------
            // For FromProto factory
            //-------------------------------------------------------------------------
            explicit LocalRelation(const spark::connect::Relation &proto);

            bool has_data() const { return proto_.local_relation().has_data(); }
            const std::string &data() const { return proto_.local_relation().data(); } // Returns raw bytes
            bool has_schema() const { return proto_.local_relation().has_schema(); }
            const std::string &schema() const { return proto_.local_relation().schema(); }
        };

        /**
         * @brief Represents a Spark Range relation.
         */
        class RangeRelation : public SparkRelation
        {
        public:
            TypeId GetTypeId() const override { return TypeId::RANGE; }

            RangeRelation(int64_t end,
                          std::optional<int64_t> start = std::nullopt,
                          std::optional<int64_t> step = std::nullopt,
                          std::optional<int32_t> num_partitions = std::nullopt);

            //-------------------------------------------------------------------------        
            // For FromProto factory
            //-------------------------------------------------------------------------
            explicit RangeRelation(const spark::connect::Relation &proto) : SparkRelation(proto) {}

            //-------------------------------------------------------------------------
            // Remove std::optional and has_X() for scalar proto3 fields
            //-------------------------------------------------------------------------
            int64_t end() const { return proto_.range().end(); }
            int64_t start() const { return proto_.range().start(); }                   // Will be 0 if not set
            int64_t step() const { return proto_.range().step(); }                     // Will be 0 if not set
            int32_t num_partitions() const { return proto_.range().num_partitions(); } // Will be 0 if not set
        };


        //-------------------------------------------------------------------------
        // Global Convenience Functions for creating relations
        //-------------------------------------------------------------------------
        
        // Example: spark.read.table("my_table")
        std::shared_ptr<ReadRelation> ReadTable(const std::string &unparsed_identifier,
                                                const std::map<std::string, std::string> &options = {},
                                                bool is_streaming = false);

        // Example: spark.read.format("csv").option("header", "true").load("path")
        std::shared_ptr<ReadRelation> ReadDataSource(const std::string &format,
                                                     const std::map<std::string, std::string> &options = {},
                                                     const std::vector<std::string> &paths = {},
                                                     std::optional<std::string> schema = std::nullopt,
                                                     const std::vector<std::string> &predicates = {},
                                                     bool is_streaming = false);

        // Example: spark.range(10)
        std::shared_ptr<RangeRelation> Range(int64_t end,
                                             std::optional<int64_t> start = std::nullopt,
                                             std::optional<int64_t> step = std::nullopt,
                                             std::optional<int32_t> num_partitions = std::nullopt);

    } // namespace client
} // namespace spark

#endif // SPARK_CONNECT_CPP_RELATIONS_H