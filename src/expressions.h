#ifndef SPARK_CONNECT_CPP_EXPRESSIONS_H
#define SPARK_CONNECT_CPP_EXPRESSIONS_H

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <optional>
#include <stdexcept>
#include <variant> // For LiteralExpression values

#include "spark/connect/expressions.pb.h"
#include "spark/connect/types.pb.h"  // For DataType in CastExpression
#include "spark/connect/common.pb.h" // For common things like errors, etc.

#include "types.h" // For SparkDataType, especially for CastExpression

namespace spark
{
    namespace client
    {
        //---------------------------------------------------------------
        // Forward declarations for all concrete SparkExpression types
        //---------------------------------------------------------------
        class SparkExpression;
        class LiteralExpression;
        class UnresolvedAttributeExpression;
        class UnresolvedFunctionExpression;
        class UnresolvedStarExpression;
        class AliasExpression;
        class CastExpression;
        // more expressions e.g. Binary expressions, logical expressions, etc. will go here

        /**
         * @brief Base abstract class for all Spark Expression types.
         * This class directly holds the spark::connect::Expression Protobuf message.
         */
        class SparkExpression
        {
        public:
            enum class TypeId
            {
                UNKNOWN,
                LITERAL,
                UNRESOLVED_ATTRIBUTE,
                UNRESOLVED_FUNCTION,
                UNRESOLVED_STAR,
                ALIAS,
                CAST,
                // more expression types
            };

            //------------------------------------------------------------------------------
            // Constructor to initialize from a Protobuf message (used by FromProto factory)
            //------------------------------------------------------------------------------
            explicit SparkExpression(const spark::connect::Expression &proto) : proto_(proto) {}

            //------------------------------------------------------------------------------
            // Default constructor for derived classes
            //------------------------------------------------------------------------------
            SparkExpression() = default;
            virtual ~SparkExpression() = default;

            //------------------------------------------------------------------------------
            // Factory method to create a SparkExpression from a Protobuf Expression
            //------------------------------------------------------------------------------
            static std::shared_ptr<SparkExpression> FromProto(const spark::connect::Expression &proto);

            //------------------------------------------------------------------------------
            // Get the underlying Protobuf message
            //------------------------------------------------------------------------------
            const spark::connect::Expression &ToProto() const { return proto_; }
            spark::connect::Expression *MutableProto() { return &proto_; } // For internal use by derived classes

            //------------------------------------------------------------------------------
            // Pure virtual method to get the specific TypeId
            //------------------------------------------------------------------------------
            virtual TypeId GetTypeId() const = 0;

            // Access common metadata for the expression (if any, though less common than relations)
            // const spark::connect::ExpressionCommon& common() const { return proto_.common(); }
            // spark::connect::ExpressionCommon* mutable_common() { return proto_.mutable_common(); }

        protected:
            spark::connect::Expression proto_; // The underlying Protobuf message
        };

        //------------------------------------------------------------------------------
        // Concrete Expression Implementations
        //------------------------------------------------------------------------------

        /**
         * @brief Represents a literal (constant) value.
         */
        class LiteralExpression : public SparkExpression
        {
        public:
            TypeId GetTypeId() const override { return TypeId::LITERAL; }

            //---------------------------------------------------------------------------------------
            // Use std::variant for flexible literal value types
            //---------------------------------------------------------------------------------------
            using ValueType = std::variant<
                bool,
                int32_t,
                int64_t,
                float,
                double,
                std::string,          // For string literals
                std::vector<uint8_t>, // For binary literals (bytes)
                std::monostate        // For null literal
                // more types e.g., Decimal, Date, Timestamp, Array, Map, Struct will go here
                >;

            //---------------------------------------------------------------------------------------
            // Constructors for various literal types
            //---------------------------------------------------------------------------------------
            explicit LiteralExpression(bool value);
            explicit LiteralExpression(int32_t value);
            explicit LiteralExpression(int64_t value);
            explicit LiteralExpression(float value);
            explicit LiteralExpression(double value);
            explicit LiteralExpression(const std::string &value);
            explicit LiteralExpression(std::string &&value);
            explicit LiteralExpression(const std::vector<uint8_t> &value);
            explicit LiteralExpression(std::vector<uint8_t> &&value);
            explicit LiteralExpression(std::nullptr_t); // For null literal

            //---------------------------------------------------------------------------------------
            // For FromProto factory
            //---------------------------------------------------------------------------------------
            explicit LiteralExpression(const spark::connect::Expression &proto);

            //---------------------------------------------------------------------------------------
            // Accessor for the value (requires checking the variant type)
            //---------------------------------------------------------------------------------------
            const ValueType &value() const { return value_; }

        private:
            ValueType value_;         // Store the C++ representation of the literal value
            void setValueFromProto(); // Helper to populate value_ from proto_
        };

        /**
         * @brief Represents an unresolved attribute (column reference by name).
         * The underlying Protobuf uses a single string for the identifier,
         * even for nested fields (e.g., "struct.field").
         */
        class UnresolvedAttributeExpression : public SparkExpression
        {
        public:
            TypeId GetTypeId() const override { return TypeId::UNRESOLVED_ATTRIBUTE; }

            //---------------------------------------------------------------------------------------
            // Constructor that takes the full unparsed identifier (e.g., "col_name" or "struct.field")
            //---------------------------------------------------------------------------------------
            explicit UnresolvedAttributeExpression(std::string unparsed_identifier,
                                                   std::optional<int64_t> plan_id = std::nullopt,
                                                   std::optional<bool> is_metadata_column = std::nullopt);

            //---------------------------------------------------------------------------------------
            // For FromProto factory
            //---------------------------------------------------------------------------------------
            explicit UnresolvedAttributeExpression(const spark::connect::Expression &proto);

            //---------------------------------------------------------------------------------------
            // Accessor for the full unparsed identifier string
            //---------------------------------------------------------------------------------------
            const std::string &unparsed_identifier() const { return proto_.unresolved_attribute().unparsed_identifier(); }

            //---------------------------------------------------------------------------------------
            // Accessors for optional fields
            //---------------------------------------------------------------------------------------
            std::optional<int64_t> plan_id() const
            {
                return proto_.unresolved_attribute().has_plan_id() ? std::make_optional(proto_.unresolved_attribute().plan_id()) : std::nullopt;
            }
            std::optional<bool> is_metadata_column() const
            {
                return proto_.unresolved_attribute().has_is_metadata_column() ? std::make_optional(proto_.unresolved_attribute().is_metadata_column()) : std::nullopt;
            }
        };

        /**
         * @brief Represents an unresolved function call (e.g., "sum(col)", "count(*)").
         */
        class UnresolvedFunctionExpression : public SparkExpression
        {
        public:
            TypeId GetTypeId() const override { return TypeId::UNRESOLVED_FUNCTION; }

            UnresolvedFunctionExpression(std::string function_name,
                                         std::vector<std::shared_ptr<SparkExpression>> arguments = {},
                                         bool is_distinct = false,
                                         bool is_user_defined_function = false);

            //---------------------------------------------------------------------------------------
            // For FromProto factory
            //---------------------------------------------------------------------------------------
            explicit UnresolvedFunctionExpression(const spark::connect::Expression &proto);

            const std::string &function_name() const { return proto_.unresolved_function().function_name(); }
            const std::vector<std::shared_ptr<SparkExpression>> &arguments() const { return arguments_; }
            bool is_distinct() const { return proto_.unresolved_function().is_distinct(); }
            bool is_user_defined_function() const { return proto_.unresolved_function().is_user_defined_function(); } // New accessor

        private:
            std::vector<std::shared_ptr<SparkExpression>> arguments_; // C++ wrappers for arguments
        };

        /**
         * @brief Represents an unresolved star expression (e.g., `*` in `SELECT *` or `alias.*`).
         */
        class UnresolvedStarExpression : public SparkExpression
        {
        public:
            TypeId GetTypeId() const override { return TypeId::UNRESOLVED_STAR; }

            //---------------------------------------------------------------------------------------
            // Constructor for '*' or 'alias.*'
            // unparsed_target can be empty for a global '*'
            //---------------------------------------------------------------------------------------
            explicit UnresolvedStarExpression(std::optional<std::string> unparsed_target = std::nullopt,
                                              std::optional<int64_t> plan_id = std::nullopt);

            //---------------------------------------------------------------------------------------
            // For FromProto factory
            //---------------------------------------------------------------------------------------
            explicit UnresolvedStarExpression(const spark::connect::Expression &proto);

            //---------------------------------------------------------------------------------------
            // Accessor for the unparsed target (e.g., "alias")
            //---------------------------------------------------------------------------------------
            std::optional<std::string> unparsed_target() const
            {
                return proto_.unresolved_star().has_unparsed_target() ? std::make_optional(proto_.unresolved_star().unparsed_target()) : std::nullopt;
            }

            //---------------------------------------------------------------------------------------
            // Accessor for optional plan_id
            //---------------------------------------------------------------------------------------
            std::optional<int64_t> plan_id() const
            {
                return proto_.unresolved_star().has_plan_id() ? std::make_optional(proto_.unresolved_star().plan_id()) : std::nullopt;
            }
        };

        /**
         * @brief Represents an alias expression (e.g., `col AS new_name`).
         */
        class AliasExpression : public SparkExpression
        {
        public:
            TypeId GetTypeId() const override { return TypeId::ALIAS; }

            //---------------------------------------------------------------------------------------
            // Constructor for alias: expr AS name (name can have multiple parts for complex types)
            //---------------------------------------------------------------------------------------
            AliasExpression(std::shared_ptr<SparkExpression> expr,
                            std::vector<std::string> name_parts, // Renamed from alias_name and qualifier
                            std::optional<std::string> metadata = std::nullopt);

            //---------------------------------------------------------------------------------------
            // For FromProto factory
            //---------------------------------------------------------------------------------------
            explicit AliasExpression(const spark::connect::Expression &proto);

            const std::shared_ptr<SparkExpression> &expr() const { return expr_; }

            //----------------------------------------------------------------------------------------
            // The proto returns RepeatedPtrField<std::string>, convert to std::vector<std::string>
            //----------------------------------------------------------------------------------------
            const std::vector<std::string> name() const
            {
                const auto &proto_names = proto_.alias().name();
                return std::vector<std::string>(proto_names.begin(), proto_names.end());
            }

            std::optional<std::string> metadata() const
            {
                return proto_.alias().has_metadata() ? std::make_optional(proto_.alias().metadata()) : std::nullopt;
            }

        private:
            std::shared_ptr<SparkExpression> expr_; // The expression being aliased
        };

        //----------------------------------------------------------------------------------------
        // Mirroring spark.connect.Cast.EvalMode for C++ use
        //----------------------------------------------------------------------------------------
        enum class CastEvalMode
        {
            UNSPECIFIED = 0,
            LEGACY = 1,
            ANSI = 2,
            TRY = 3
        };

        /**
         * @brief Represents a cast expression (e.g., `CAST(col AS INT)`).
         */
        class CastExpression : public SparkExpression
        {
        public:
            TypeId GetTypeId() const override { return TypeId::CAST; }

            //---------------------------------------------------------------------------------------
            // Constructor for cast using a SparkDataType object
            // For now, this wrapper primarily supports casting to a structured SparkDataType.
            // If the proto contains `type_str`, the FromProto factory will throw an error.
            //---------------------------------------------------------------------------------------
            CastExpression(std::shared_ptr<SparkExpression> expr,
                           std::shared_ptr<SparkDataType> data_type,
                           CastEvalMode eval_mode = CastEvalMode::UNSPECIFIED);

            //----------------------------------------------------------------------------------------
            // For FromProto factory
            //----------------------------------------------------------------------------------------
            explicit CastExpression(const spark::connect::Expression &proto);

            const std::shared_ptr<SparkExpression> &expr() const { return expr_; }
            const std::shared_ptr<SparkDataType> &data_type() const { return data_type_; } // Accessor for type based on oneof
            CastEvalMode eval_mode() const;                                                // Accessor for EvalMode

            //----------------------------------------------------------------------------------------
            // Accessors for the 'type_str' part of the oneof (if present in proto)
            //----------------------------------------------------------------------------------------
            bool has_type_str() const { return proto_.cast().has_type_str(); }
            const std::string &type_str() const { return proto_.cast().type_str(); }

        private:
            std::shared_ptr<SparkExpression> expr_;
            std::shared_ptr<SparkDataType> data_type_; // Stores the C++ representation of the target DataType
        };

        //----------------------------------------------------------------------------------------
        // Global Convenience Functions for creating expressions
        //----------------------------------------------------------------------------------------

        //----------------------------------------------------------------------------------------
        // Example: col("my_column") or col("struct.field")
        //----------------------------------------------------------------------------------------
        std::shared_ptr<UnresolvedAttributeExpression> col(const std::string &unparsed_identifier,
                                                           std::optional<int64_t> plan_id = std::nullopt,
                                                           std::optional<bool> is_metadata_column = std::nullopt);

        //----------------------------------------------------------------------------------------
        // Example: lit(5), lit("hello")
        //----------------------------------------------------------------------------------------
        template <typename T>
        std::shared_ptr<LiteralExpression> lit(T value)
        {
            return std::make_shared<LiteralExpression>(std::forward<T>(value));
        }

        //----------------------------------------------------------------------------------------
        // Specialization for const char* to std::string
        //----------------------------------------------------------------------------------------
        inline std::shared_ptr<LiteralExpression> lit(const char *value)
        {
            return std::make_shared<LiteralExpression>(std::string(value));
        }

        //----------------------------------------------------------------------------------------
        // For null
        //----------------------------------------------------------------------------------------
        inline std::shared_ptr<LiteralExpression> lit_null()
        {
            return std::make_shared<LiteralExpression>(nullptr);
        }

        //----------------------------------------------------------------------------------------
        // Example: sum("col"), count("*")
        //----------------------------------------------------------------------------------------
        std::shared_ptr<UnresolvedFunctionExpression> call_function(
            const std::string &name,
            std::vector<std::shared_ptr<SparkExpression>> args = {},
            bool is_distinct = false,
            bool is_user_defined_function = false); // Added this parameter

        //----------------------------------------------------------------------------------------
        // Example: `star()` for `*`, `star("alias")` for `alias.*`
        //----------------------------------------------------------------------------------------
        std::shared_ptr<UnresolvedStarExpression> star(std::optional<std::string> unparsed_target = std::nullopt,
                                                       std::optional<int64_t> plan_id = std::nullopt);

        //----------------------------------------------------------------------------------------
        // Example: expr.as({"new_name"}), expr.as({"struct_col", "field_name"})
        //----------------------------------------------------------------------------------------
        std::shared_ptr<AliasExpression> alias(std::shared_ptr<SparkExpression> expr,
                                               std::vector<std::string> name_parts,
                                               std::optional<std::string> metadata = std::nullopt);

        //----------------------------------------------------------------------------------------
        // Example: cast(expr, int_type()), cast(expr, string_type(), CastEvalMode::ANSI)
        //----------------------------------------------------------------------------------------
        std::shared_ptr<CastExpression> cast(std::shared_ptr<SparkExpression> expr,
                                             std::shared_ptr<SparkDataType> data_type,
                                             CastEvalMode eval_mode = CastEvalMode::UNSPECIFIED);

    } // namespace client
} // namespace spark

#endif // SPARK_CONNECT_CPP_EXPRESSIONS_H