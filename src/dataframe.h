#pragma once

#include <string>
#include <memory>
#include <spark/connect/base.grpc.pb.h>
#include <spark/connect/relations.pb.h>

#include <types.h>

class DataFrameWriter;

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
    spark::sql::types::StructType schema() const;

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
     */
    DataFrame select(const std::vector<std::string> &cols);

    /**
     * @brief Filters rows using the given SQL condition string.
     * @param condition SQL expression (e.g., "age > 3")
     */
    DataFrame filter(const std::string &condition);

    /**
     * @brief Alias for filter().
     */
    DataFrame where(const std::string &condition);

    /**
     * @brief Returns the first n rows.
     */
    std::vector<spark::sql::types::Row> take(int n);

    /**
     * @brief Returns the first row.
     */
    std::optional<spark::sql::types::Row> head();

    /**
     * @brief Returns the first n rows.
     */
    std::vector<spark::sql::types::Row> head(int n);

    /**
     * @brief Returns a new DataFrame by taking the first n rows.
     */
    DataFrame limit(int n);

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

private:
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
    spark::connect::Plan plan_;
    std::string session_id_;
    std::string user_id_;
};