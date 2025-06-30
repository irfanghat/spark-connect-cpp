#ifndef SPARK_CONNECT_CPP_EXPRESSIONS_H
#define SPARK_CONNECT_CPP_EXPRESSIONS_H

#include <memory>

//-----------------------------------------
// Forward declare the Protobuf message
//-----------------------------------------
namespace spark
{
    namespace connect
    {
        class Expression;
    }
} // namespace spark

namespace spark
{
    namespace client
    {
        class SparkExpression
        {
        public:
            virtual const spark::connect::Expression &ToProto() const;

            SparkExpression() = default;
            virtual ~SparkExpression() = default;

            static std::shared_ptr<SparkExpression> FromProto(const spark::connect::Expression &proto);
        };

        // class LiteralExpression;
        // class ColumnExpression;
        // class BinaryExpression;

    } // namespace client
} // namespace spark

#endif // SPARK_CONNECT_CPP_EXPRESSIONS_H