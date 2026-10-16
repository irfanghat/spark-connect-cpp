#pragma once

#include "types.h"

#include <memory>
#include <string>
#include <vector>

namespace spark::connect
{
class Expression;
}

namespace spark::sql::functions
{

/**
 * @brief The Column class represents a Spark Expression (DSL).
 * We keep it here because it's the primary tool for 'functions'.
 */
struct Column
{
    std::shared_ptr<spark::connect::Expression> expr;

    explicit Column(std::string name);
    explicit Column(spark::connect::Expression e);

    // ------------------
    // DSL Operators
    // ------------------
    Column operator+(Column const& other) const;
    Column operator-(Column const& other) const;
    Column operator*(Column const& other) const;
    Column operator/(Column const& other) const;
    Column operator==(Column const& other) const;
    Column operator>(Column const& other) const;
    Column operator<(Column const& other) const;

    Column alias(std::string const& name) const;
    Column otherwise(Column const& value) const;
    Column cast(std::string const& type) const;
};

/**
 * @brief Returns a `Column` based on the given column name.
 */
Column col(std::string const& name);

/**
 * @brief Creates a `Column` of literal value.
 */
Column lit(int32_t value);
Column lit(int64_t value);
Column lit(double value);
Column lit(std::string const& value);

/**
 * @brief Converts a string expression to lower case.
 */
Column lower(Column const& e);

/**
 * @brief Concatenates multiple input string columns together into a single
 * string column, using the given separator.
 */
Column concat_ws(std::string const& sep, std::vector<Column> const& cols);

/**
 * @brief Evaluates a list of conditions and returns one of multiple possible
 * result expressions.
 */
Column when(Column const& condition, Column const& value);

} // namespace spark::sql::functions