#pragma once

#include <memory>
#include <string>
#include <vector>

#include <spark/connect/base.grpc.pb.h>
#include <spark/connect/relations.pb.h>

#include "functions.h"
#include "types.h"

using namespace spark::sql::types;

namespace graphframes
{
    class GraphFrame;
}

class DataFrameWriter;
class GroupedData;

/**
 * @class DataFrame
 * @brief Represents a distributed collection of data organized into named columns.
 *
 * This class is the primary entry point for working with data in a Spark Connect
 * application. It provides methods to inspect the data and trigger execution
 * of the underlying logical plan. It is immutable and transformations on a DataFrame
 * return a new DataFrame with an updated plan.
 */
class DataFrame
{
public:
    /**
     * @brief Constructs a new DataFrame.
     * @param stub A shared pointer to the gRPC stub for communicating with the Spark server.
     * @param plan The logical plan representing the DataFrame's transformations.
     * @param session_id The ID of the current Spark session.
     * @param user_id The ID of the user associated with the session.
     */
    DataFrame(std::shared_ptr<spark::connect::SparkConnectService::Stub> stub,
              spark::connect::Plan plan,
              std::string session_id,
              std::string user_id);

    /**
     * @brief Gets the service stub. Returns by value to share ownership.
     */
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub() const {
        return stub_;
    }

    /**
     * @brief Gets the current plan. Returns by const reference to avoid copies.
     */
    const spark::connect::Plan& plan() const {
        return plan_;
    }

    /**
     * @brief Gets the active Session ID.
     */
    const std::string& session_id() const {
        return session_id_;
    }

    /**
     * @brief Gets the User ID associated with the client.
     */
    const std::string& user_id() const {
        return user_id_;
    }

    /**
     * @brief Displays the contents of the DataFrame in a tabular format.
     *
     * This method prints the data returned from a Spark SQL query or transformation.
     * Internally, it deserializes the Arrow RecordBatch received from the Spark server
     * and formats it for terminal output.
     *
     * @param limit The maximum number of rows to display. If not provided, all rows are shown.
     *              Defaults to 10 if available.
     *
     * @note Supports pretty-printing of various data types including:
     *       - Integers and floating-point values
     *       - Strings
     *       - Booleans
     *       - Dates and timestamps (with formatting)
     *       - Nulls (displayed as "null")
     *
     * @example
     * SparkSession spark(...);
     * auto df = spark.sql("SELECT * FROM range(10)");
     * df.show(5);  // Display first 5 rows
     */
    void show(int max_rows = 10, int runcate = 20);

    /**
     * @brief Returns the schema of this DataFrame as a StructType.
     * @return A StructType object containing the full schema metadata.
     */
    StructType schema() const;

    /**
     * @brief Prints out the schema in the tree format.
     */
    void printSchema() const;

    /**
     * @brief Return the column names of the DataFrame.
     *
     * This method retrieves the schema of the DataFrame from the Spark server
     * and extracts the names of all columns in their original order.
     * @return A vector of strings representing the column names.
     *
     * @throws std::runtime_error If the schema cannot be retrieved from the server.
     *
     * @note This operation requires a round-trip to the Spark server via AnalyzePlan RPC.
     * @note Column order is preserved as defined in the DataFrame schema.
     */
    std::vector<std::string> columns() const;

    /**
     * @brief Projects a set of expressions and returns a new DataFrame.
     * @param cols A vector of column names.
     * @example auto df_subset = df.select({"b", "a"});
     */
    DataFrame select(const std::vector<std::string> &cols) const;

    /**
     * @brief Projects a set of `Column` expressions and returns a new DataFrame.
     * @param cols A vector of `Column` objects (supporting aliases, math, etc.).
     * @example auto filtered_df = df.filter(col("name") == lit("Alice"))
     *                     .select({col("name"), _col.alias("age_plus_one")});
     */
    DataFrame select(const std::vector<spark::sql::functions::Column> &cols) const;

    DataFrame select(std::initializer_list<std::string> cols) const;

    /**
     * @brief Filters rows using the given SQL condition string.
     * @param condition SQL expression (e.g., "age > 3")
     */
    DataFrame filter(const std::string &condition) const;

    /**
     * @brief Filters rows using a Column expression.
     * @param condition Column expression (e.g., col("age") > lit(21))
     */
    DataFrame filter(const spark::sql::functions::Column &condition) const;

    /**
     * @brief Alias for filter().
     */
    DataFrame where(const std::string &condition) const;

    /**
     * @brief Returns the first n rows.
     */
    std::vector<Row> take(int n);

    /**
     * @brief Returns the first row.
     */
    std::optional<Row> head();

    /**
     * @brief Returns the first n rows.
     */
    std::vector<Row> head(int n);

    /**
     * @brief Returns the first row.
     */
    std::optional<Row> first();

    /**
     * @brief Returns a new DataFrame by taking the first n rows.
     */
    DataFrame limit(int n) const;

    /**
     * @brief Returns the number of rows in this DataFrame.
     * @return The row count.
     */
    int64_t count();

    /**
     * @brief Interface to save the content of the non-streaming DataFrame out into external storage.
     * @return A DataFrameWriter instance.
     */
    DataFrameWriter write();

    /**
     * @brief Returns a new DataFrame with duplicate rows removed - equivalent to `distinct()` function
     */

    DataFrame dropDuplicates() const;
    /**
     * @brief Returns a new DataFrame with duplicate rows removed,
     *          considering only the given subset of columns - equivalent to `distinct()` function
     */
    DataFrame dropDuplicates(const std::vector<std::string> &subset) const;

    /**
     * @brief Alias for `dropDuplicates()`.
     */
    DataFrame drop_duplicates();

    /**
     * @brief Alias for `dropDuplicates(subset)`.
     */
    DataFrame drop_duplicates(const std::vector<std::string> &subset);

    /**
     * @brief Returns all the records as a list of `Row`
     * @example
     * SparkSession spark(...);
     * auto df = spark.read()
     *                  .option("header", "true");
     *                  .option("inferSchema", "true");
     *                  .csv("datasets/people.csv");
     *
     * auto rows = df.collect();
     *
     * for (auto &row : rows) {
     *  std::cout << row << std::endl;
     * }
     *
     * // ------------------------------------------
     * // Output:
     * // Row(name='John', age=25, salary=100000)
     * // Row(name='Alice', age=30, salary=85000)
     * // ...
     * // ------------------------------------------
     * @returns A list of rows.
     */
    std::vector<Row> collect();

    /**
     * @brief Returns a new `DataFrame` containing the distinct rows.
     */
    DataFrame distinct();

    /**
     * @brief Computes basic statistics for numeric and string columns.
     * @note This function is meant for exploratory data analysis, as we make no
     * guarantee about the backward compatibility of the schema of the resulting `DataFrame`.
     * Use `summary` for expanded statistics and control over which statistics to compute.
     * @param cols Column name or list of column names to describe by (default All columns).
     * @example
     * df.describe({"age"}).show();
     *
     *  +-------+----+
     *  |summary| age|
     *  +-------+----+
     *  |  count|   3|
     *  |   mean|12.0|
     *  | stddev| 1.0|
     *  |    min|  11|
     *  |    max|  13|
     *  +-------+----+
     * @returns A new DataFrame that describes (provides statistics) given DataFrame.
     */
    DataFrame describe(const std::vector<std::string> &cols);

    /**
     * @brief Computes specified statistics for numeric and string columns.
     * Available statistics are:
     * - count
     *
     * - mean
     *
     * - stddev
     *
     * - min
     *
     * - max
     *
     * - arbitrary approximate percentiles specified as a percentage (e.g., 75%)
     *
     * If no statistics are given, this function computes count, mean, stddev, min,
     * approximate quartiles (percentiles at 25%, 50%, and 75%), and max.
     *
     * @note This function is meant for exploratory data analysis, as we make no
     * guarantee about the backward compatibility of the schema of
     * the resulting `DataFrame`.
     *
     * @param statistics Column names to calculate statistics by (default All columns).
     * @returns A new `DataFrame` that provides statistics for the given `DataFrame`.
     */
    DataFrame summary(const std::vector<std::string> &statistics);

    /**
     * @brief Overload for empty summary() which computes the default
     * set of statistics (count, mean, stddev, min, 25%, 50%, 75%, max).
     */
    DataFrame summary();

    /**
     * @brief Performs an inner join with another DataFrame on all columns with matching names.
     *
     * This function automatically finds all columns that exist in both DataFrames
     * and performs an inner join on them. If there are no common columns,
     * an exception is thrown.
     *
     * @param other The right side of the join.
     * @return A new DataFrame representing the inner join on common columns.
     * @throws std::invalid_argument if there are no common columns to join on.
     */
    DataFrame join(const DataFrame &other) const;

    /**
     * @brief Joins this DataFrame with another DataFrame using a join column name.
     *
     * Performs an equi-join using the specified column. The column must exist
     * in both DataFrames.
     *
     * @param other The right side of the join.
     * @param on Column name to join on.
     * @param how Type of join. Default is "inner".
     *
     * Must be one of:
     * `"inner"`, `"cross"`,
     * `"outer"`, `"full"`, `"fullouter"`, `"full_outer"`,
     * `"left"`, `"leftouter"`, `"left_outer"`,
     * `"right"`, `"rightouter"`, `"right_outer"`,
     * `"semi"`, `"leftsemi"`, `"left_semi"`,
     * `"anti"`, `"leftanti"`, `"left_anti"`.
     *
     * @return A new DataFrame representing the joined result.
     */
    DataFrame join(const DataFrame &other,
                   const std::string &on,
                   const std::string &how = "inner");

    /**
     * @brief Joins this DataFrame with another DataFrame using multiple column names.
     *
     * Performs an equi-join using the specified list of columns.
     * All columns must exist in bboth DataFrames.
     *
     * @param other The right side of the join.
     * @param on List of colum names to join on.
     * @param how Type of join. Default is "inner".
     *
     * @return A new DataFrame representing the joined result.
     */

    DataFrame join(const DataFrame &other,
                   const std::vector<std::string> &on,
                   const std::string &how = "inner");

    /**
     * @brief Joins this DataFrame with another DataFrame using a SQL expression.
     *
     * @param other The right side of the join.
     * @param condition SQL join expression (e.g., "df1.id = df2.id").
     * @param how Type of join. Default is "inner".
     *
     * @return A new DataFrame representing the joined result.
     */
    DataFrame join_on_expression(const DataFrame &other,
                                 const std::string &condition,
                                 const std::string &how);

    /**
     * @brief Groups the DataFrame using the specified columns.
     */
    GroupedData groupBy(const std::vector<std::string> &cols);
    GroupedData groupBy(const std::vector<spark::sql::functions::Column> &cols);
    GroupedData groupBy(std::initializer_list<std::string> cols);
    GroupedData groupBy();

    /**
     * @brief Returns a new `DataFrame`. Concise syntax for chaining custom transformations.
     * @tparam F A callable type that takes a DataFrame as the first argument.
     * @tparam Args Additional argument types to pass to the function.
     * @param func A function that takes a DataFrame and returns a DataFrame.
     * @param args Optional additional arguments to pass to func.
     * @return A transformed `DataFrame`.
     */
    template <typename F, typename... Args>
    DataFrame transform(F func, Args &&...args) const
    {
        // --------------------------------------------------------------------
        // Invoke the function with this DataFrame and any additional arguments
        // --------------------------------------------------------------------
        return func(*this, std::forward<Args>(args)...);
    }

    /**
     * @brief Returns a new `DataFrame` by adding a column or replacing the existing column that has the same name.
     *
     * The column expression must be an expression over this `DataFrame`, attempting to add a column from some other DataFrame will raise an error.
     *
     * @param colName The name of the new column.
     * @param col The column expression for the new column.
     */
    DataFrame withColumn(const std::string &colName, const spark::sql::functions::Column &col) const;

private:
    friend class GroupedData;
    friend class graphframes::GraphFrame;

    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
    spark::connect::Plan plan_;
    std::string session_id_;
    std::string user_id_;
};