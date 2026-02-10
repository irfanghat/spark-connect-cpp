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
#include "reader.h"

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
 */
class SparkSession
{
public:
    /**
     * @class Builder
     * @brief A builder class to construct and configure a SparkSession.
     */
    class Builder
    {
    public:
        /**
         * @brief Sets the master URL for the SparkSession.
         * @param master The master URL (e.g., "adb-xxx.azuredatabricks.net").
         * @return A reference to the Builder for chaining calls.
         */
        Builder &master(const std::string &master)
        {
            config_.setHost(master);
            return *this;
        }

        /**
         * @brief Sets the application name for the SparkSession.
         * @param appName The name of the application.
         * @return A reference to the Builder for chaining calls.
         */
        Builder &appName(const std::string &appName)
        {
            config_.setUserId(appName);
            return *this;
        }

        /**
         * @brief Enables or disables SSL for the connection.
         * @param use_ssl True to enable SSL (This is required for Databricks interactions).
         */
        Builder &use_ssl(bool use_ssl = true)
        {
            config_.setUseSSL(use_ssl);
            return *this;
        }

        /**
         * @brief Configures the session for Databricks connectivity.
         * @param token Personal Access Token (PAT).
         * @param cluster_id The Databricks cluster ID.
         */
        Builder &databricks(const std::string &token, const std::string &cluster_id)
        {
            config_.setDatabricksAuth(token, cluster_id);
            return *this;
        }

        /**
         * @brief Configures the session for Databricks Serverless (SQL Warehouse).
         * @param token Personal Access Token (PAT).
         * @param warehouse_id The SQL Warehouse ID.
         */
        Builder &serverless(const std::string &token, const std::string &warehouse_id)
        {
            config_.setServerlessAuth(token, warehouse_id);
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
    DataFrame range(int64_t start, int64_t end, int64_t step = 1);
    DataFrame range(int64_t end);
    DataFrameReader read();

    /**
     * @brief Creates a new SparkSession with an isolated session ID.
     * @return A new SparkSession instance.
     */
    SparkSession newSession();

    /**
     * @brief Stops the underlying Spark session.
     */
    void stop();

    /**
     * @brief Access the session configuration.
     */
    Config &conf() { return config_; }

    std::string session_id() const { return config_.session_id; }
    std::string user_id() const { return config_.user_id; }

private:
    explicit SparkSession(const Config &config);

    /**
     * @brief Singleton instance and mutex for thread-safe access
     */
    static SparkSession *instance_;
    static std::once_flag once_flag_;

    /**
     * @brief  Internal Spark Configuration
     */
    Config config_;
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
};