#include "functions.h"

#include "types.h"

#include <spark/connect/expressions.pb.h>

namespace spark::sql::functions
{

using namespace spark::sql::types;

Column::Column(std::string name)
    : expr(std::make_shared<spark::connect::Expression>())
{
    expr->mutable_unresolved_attribute()->set_unparsed_identifier(name);
}

Column::Column(spark::connect::Expression e)
    : expr(std::make_shared<spark::connect::Expression>(std::move(e)))
{
}

/**
 * @brief Helper to build Spark's UnresolvedFunction calls
 */
static Column
build_binary_op(std::string const& op, Column const& l, Column const& r)
{
    spark::connect::Expression e;
    auto* func = e.mutable_unresolved_function();
    func->set_function_name(op);
    *func->add_arguments() = *l.expr;
    *func->add_arguments() = *r.expr;
    return Column(std::move(e));
}

Column
Column::operator+(Column const& other) const
{
    return build_binary_op("+", *this, other);
}
Column
Column::operator-(Column const& other) const
{
    return build_binary_op("-", *this, other);
}
Column
Column::operator*(Column const& other) const
{
    return build_binary_op("*", *this, other);
}
Column
Column::operator/(Column const& other) const
{
    return build_binary_op("/", *this, other);
}
Column
Column::operator==(Column const& other) const
{
    return build_binary_op("==", *this, other);
}
Column
Column::operator>(Column const& other) const
{
    return build_binary_op(">", *this, other);
}
Column
Column::operator<(Column const& other) const
{
    return build_binary_op("<", *this, other);
}

Column
Column::alias(std::string const& name) const
{
    spark::connect::Expression e;
    auto* a = e.mutable_alias();
    *a->mutable_expr() = *this->expr;
    a->add_name(name);
    return Column(std::move(e));
}

Column
Column::otherwise(Column const& value) const
{
    spark::connect::Expression e;
    auto* func = e.mutable_unresolved_function();

    func->set_function_name("when");

    // -------------------------------------------------------------------
    // Copy existing arguments from the 'when' call if they exist
    // -------------------------------------------------------------------
    if (this->expr->has_unresolved_function())
    {
        for (auto const& arg : this->expr->unresolved_function().arguments())
        {
            *func->add_arguments() = arg;
        }
    }

    *func->add_arguments() = *value.expr;
    return Column(std::move(e));
}

Column
Column::cast(std::string const& type) const
{
    spark::connect::Expression e;
    auto* cast = e.mutable_cast();
    *cast->mutable_expr() = *this->expr;
    cast->mutable_type()->mutable_unparsed()->set_data_type_string(type);
    return Column(std::move(e));
}

Column
lower(Column const& e)
{
    spark::connect::Expression expr;
    auto* func = expr.mutable_unresolved_function();
    func->set_function_name("lower");
    *func->add_arguments() = *e.expr;
    return Column(std::move(expr));
}

Column
col(std::string const& name)
{
    return Column(name);
}

Column
lit(int32_t value)
{
    spark::connect::Expression e;
    e.mutable_literal()->set_integer(value);
    return Column(std::move(e));
}

Column
lit(int64_t value)
{
    spark::connect::Expression e;
    e.mutable_literal()->set_integer(value);
    return Column(std::move(e));
}

Column
lit(double value)
{
    spark::connect::Expression e;
    e.mutable_literal()->set_double_(value);
    return Column(std::move(e));
}

Column
lit(std::string const& value)
{
    spark::connect::Expression e;
    e.mutable_literal()->set_string(value);
    return Column(std::move(e));
}

Column
concat_ws(std::string const& sep, std::vector<Column> const& cols)
{
    spark::connect::Expression e;
    auto* func = e.mutable_unresolved_function();
    func->set_function_name("concat_ws");

    // -------------------------------------------------
    // Assign the Protobuf Expression gotten via .expr
    // -------------------------------------------------
    *func->add_arguments() = *lit(sep).expr;
    for (auto const& c : cols)
    {
        *func->add_arguments() = *c.expr;
    }
    return Column(std::move(e));
}

Column
when(Column const& condition, Column const& value)
{
    spark::connect::Expression e;
    auto* func = e.mutable_unresolved_function();
    func->set_function_name("when");
    *func->add_arguments() = *condition.expr;
    *func->add_arguments() = *value.expr;
    return Column(std::move(e));
}
} // namespace spark::sql::functions