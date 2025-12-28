#pragma once

#include <string>
#include <memory>
#include <map>
#include <mutex>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include <spark/connect/base.grpc.pb.h>
#include "config.h"
#include "dataframe.h"
#include "dataframe_reader.h"

class DataFrameReader;

/**
 * @class SparkSession
 * @brief The entry point to programming Spark with the Dataset and DataFrame API.
 *
 * This class serves as the central point of interaction with Spark. It can be used
 * to create DataFrames, execute SQL queries, and manage the session lifecycle.
 *
 * The class is designed to be created using the builder pattern, mirroring the
 * PySpark API.
 *
 * @example
 * // Create a SparkSession using the builder pattern
 * SparkSession spark = SparkSession::builder()
 * .master("sc://localhost:15002")
 * .appName("My Cpp App")
 * .getOrCreate();
 *
 * // Execute a SQL query
 * DataFrame df = spark.sql("SELECT * FROM range(10)");
 * df.show();
 */
class SparkSession
{
public:
    /**
     * @class Builder
     * @brief A builder class to construct and configure a SparkSession.
     *
     * This class allows for flexible configuration of a SparkSession instance
     * before it is created or retrieved.
     */
    class Builder
    {
    public:
        /**
         * @brief Sets the master URL for the SparkSession.
         * @param master The master URL, e.g., "sc://localhost:15002".
         * @return A reference to the Builder for chaining calls.
         */
        Builder &master(const std::string &master)
        {
            config_.host = master;
            return *this;
        }

        /**
         * @brief Sets the application name for the SparkSession.
         * @param appName The name of the application.
         * @return A reference to the Builder for chaining calls.
         */
        Builder &appName(const std::string &appName)
        {
            config_.user_id = appName;
            return *this;
        }

        /**
         * @brief Gets or creates a new SparkSession with the specified configurations.
         * @return The singleton SparkSession instance.
         */
        SparkSession &getOrCreate();

    private:
        Config config_;
    };

    static Builder builder()
    {
        return Builder();
    }

    DataFrame sql(const std::string &query);
    DataFrame range(int64_t end);
    DataFrameReader read();
    // Placeholder for a more complex method (Review PySpark implementation)
    // DataFrame createDataFrame(const std::vector<std::vector<std::string>>& data, const std::vector<std::string>& schema);

    /**
     * @brief Creates a new SparkSession with an isolated session ID.
     * @return A new SparkSession instance.
     */
    SparkSession newSession();

    /**
     * @brief Stops the underlying Spark session.
     */
    void stop();

    std::string session_id() const { return config_.session_id; }
    std::string user_id() const { return config_.user_id; }

private:
    explicit SparkSession(const Config &config);

    // -----------------------------------------------------
    // Singleton instance and mutex for thread-safe access
    // -----------------------------------------------------
    static SparkSession *instance_;
    static std::once_flag once_flag_;

    // -----------------------------------------------------
    // Member variables
    // -----------------------------------------------------
    Config config_;
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
};