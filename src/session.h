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
#include "runtime_config.h"

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
         * @param master The master URL (e.g., "sc://localhost" or "adb-xxx.azuredatabricks.net").
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
         * @brief Sets a Spark runtime configuration key-value pair.
         *
         * @see PySpark SparkSession.builder.config() API.
         * Configs are applied to the session immediately after it is created.
         *
         * @example
         * SparkSession::builder()
         *     .master("sc://localhost")
         *     .config("spark.sql.shuffle.partitions", "8")
         *     .config("spark.executor.memory", "4g")
         *     .getOrCreate();
         */
        Builder &config(const std::string &key, const std::string &value)
        {
            config_.setRuntimeConfig(key, value);
            return *this;
        }

        Builder &config(const std::string &key, const char *value)
        {
            config_.setRuntimeConfig(key, std::string(value));
            return *this;
        }

        Builder &config(const std::string &key, bool value)
        {
            config_.setRuntimeConfig(key, value ? "true" : "false");
            return *this;
        }

        Builder &config(const std::string &key, int64_t value)
        {
            config_.setRuntimeConfig(key, std::to_string(value));
            return *this;
        }

        Builder &config(const std::string &key, int value)
        {
            config_.setRuntimeConfig(key, std::to_string(value));
            return *this;
        }

        Builder &config(const std::string &key, double value)
        {
            config_.setRuntimeConfig(key, std::to_string(value));
            return *this;
        }

        Builder &config(const std::map<std::string, std::string> &configs)
        {
            for (const auto &[key, value] : configs)
                config_.setRuntimeConfig(key, value);
            return *this;
        }

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
     * @brief Returns a RuntimeConfig for reading and writing Spark session config.
     *
     * Changes are applied on the server and survive across DataFrame operations within this session.
     *
     * @example
     * spark.conf().set("spark.sql.shuffle.partitions", "8");
     *
     * auto val = spark.conf().get("spark.sql.shuffle.partitions");
     */
    RuntimeConfig conf();

    /**
     * @brief Sets the Spark checkpoint directory for this session.
     *
     * Required by algorithms that use checkpointing internally, such as
     * GraphFrames ConnectedComponents. Equivalent to calling `spark.conf().set("spark.checkpoint.dir", path)`
     *
     * @param path A path writable by the Spark executors (e.g. `/tmp/checkpoints`,
     *             `hdfs:///checkpoints`, or an `ABFS/S3` URI for cloud deployments).
     */
    void setCheckpointDir(const std::string &path);

    /**
     * @brief Access the internal connection configuration (host, port, auth, etc.)
     */
    Config &connection() { return config_; }

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
     * @brief  Internal Spark connection configuration
     */
    Config config_;
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
};