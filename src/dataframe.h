#pragma once

#include <string>
#include <memory>
#include <spark/connect/base.grpc.pb.h>
#include <spark/connect/relations.pb.h>

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
     * @brief Displays the top rows of the DataFrame in a tabular format.
     *
     * This method triggers the execution of the DataFrame's logical plan and
     * fetches the results from the Spark server.
     *
     * @param max_rows The maximum number of rows to display. Defaults to 10.
     */
    void show(int max_rows = 10);

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

private:
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
    spark::connect::Plan plan_;
    std::string session_id_;
    std::string user_id_;
};