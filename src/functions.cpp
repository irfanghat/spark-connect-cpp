#include "functions.h"
#include "types.h"

#include <spark/connect/expressions.pb.h>

namespace spark::sql::functions
{

    using namespace spark::sql::types;

    Column::Column(std::string name) : expr(std::make_shared<spark::connect::Expression>())
    {
        expr->mutable_unresolved_attribute()->set_unparsed_identifier(name);
    }

    Column::Column(spark::connect::Expression e) : expr(std::make_shared<spark::connect::Expression>(std::move(e))) {}

    /**
     * @brief Helper to build Spark's UnresolvedFunction calls
     */
    static Column build_binary_op(const std::string &op, const Column &l, const Column &r)
    {
        spark::connect::Expression e;
        auto *func = e.mutable_unresolved_function();
        func->set_function_name(op);
        *func->add_arguments() = *l.expr;
        *func->add_arguments() = *r.expr;
        return Column(std::move(e));
    }

    Column Column::operator+(const Column &other) const { return build_binary_op("+", *this, other); }
    Column Column::operator-(const Column &other) const { return build_binary_op("-", *this, other); }
    Column Column::operator*(const Column &other) const { return build_binary_op("*", *this, other); }
    Column Column::operator/(const Column &other) const { return build_binary_op("/", *this, other); }
    Column Column::operator==(const Column &other) const { return build_binary_op("==", *this, other); }
    Column Column::operator>(const Column &other) const { return build_binary_op(">", *this, other); }
    Column Column::operator<(const Column &other) const { return build_binary_op("<", *this, other); }

    Column Column::alias(const std::string &name) const
    {
        spark::connect::Expression e;
        auto *a = e.mutable_alias();
        *a->mutable_expr() = *this->expr;
        a->add_name(name);
        return Column(std::move(e));
    }

    Column Column::otherwise(const Column &value) const
    {
        spark::connect::Expression e;
        auto *func = e.mutable_unresolved_function();

        func->set_function_name("when");

        // -------------------------------------------------------------------
        // Copy existing arguments from the 'when' call if they exist
        // -------------------------------------------------------------------
        if (this->expr->has_unresolved_function())
        {
            for (const auto &arg : this->expr->unresolved_function().arguments())
            {
                *func->add_arguments() = arg;
            }
        }

        *func->add_arguments() = *value.expr;
        return Column(std::move(e));
    }

    Column lower(const Column &e)
    {
        spark::connect::Expression expr;
        auto *func = expr.mutable_unresolved_function();
        func->set_function_name("lower");
        *func->add_arguments() = *e.expr;
        return Column(std::move(expr));
    }

    Column col(const std::string &name) { return Column(name); }

    Column lit(int32_t value)
    {
        spark::connect::Expression e;
        e.mutable_literal()->set_integer(value);
        return Column(std::move(e));
    }

    Column lit(double value)
    {
        spark::connect::Expression e;
        e.mutable_literal()->set_double_(value);
        return Column(std::move(e));
    }

    Column lit(const std::string &value)
    {
        spark::connect::Expression e;
        e.mutable_literal()->set_string(value);
        return Column(std::move(e));
    }

    Column concat_ws(const std::string &sep, const std::vector<Column> &cols)
    {
        spark::connect::Expression e;
        auto *func = e.mutable_unresolved_function();
        func->set_function_name("concat_ws");

        // -------------------------------------------------
        // Assign the Protobuf Expression gotten via .expr
        // -------------------------------------------------
        *func->add_arguments() = *lit(sep).expr;
        for (const auto &c : cols)
        {
            *func->add_arguments() = *c.expr;
        }
        return Column(std::move(e));
    }

    Column when(const Column &condition, const Column &value)
    {
        spark::connect::Expression e;
        auto *func = e.mutable_unresolved_function();
        func->set_function_name("when");
        *func->add_arguments() = *condition.expr;
        *func->add_arguments() = *value.expr;
        return Column(std::move(e));
    }
}