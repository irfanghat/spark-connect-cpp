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
    Column operator+(const Column& other) const;
    Column operator-(const Column& other) const;
    Column operator*(const Column& other) const;
    Column operator/(const Column& other) const;
    Column operator==(const Column& other) const;
    Column operator>(const Column& other) const;
    Column operator<(const Column& other) const;

    Column alias(const std::string& name) const;
    Column otherwise(const Column& value) const;
    Column cast(const std::string& type) const;
};

/**
 * @brief Returns a `Column` based on the given column name.
 */
Column col(const std::string& name);

/**
 * @brief Creates a `Column` of literal value.
 */
Column lit(int32_t value);
Column lit(int64_t value);
Column lit(double value);
Column lit(const std::string& value);

/**
 * @brief Converts a string expression to lower case.
 */
Column lower(const Column& e);

/**
 * @brief Concatenates multiple input string columns together into a single
 * string column, using the given separator.
 */
Column concat_ws(const std::string& sep, const std::vector<Column>& cols);

/**
 * @brief Evaluates a list of conditions and returns one of multiple possible
 * result expressions.
 */
Column when(const Column& condition, const Column& value);

} // namespace spark::sql::functions