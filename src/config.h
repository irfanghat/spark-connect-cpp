#pragma once

#include <map>
#include <string>
#include <vector>

/**
 * @class Config
 * @brief Internal configuration state for the Spark connection.
 */
class Config
{
  public:
    std::string host = "localhost";
    int port = 15002;
    std::string session_id;
    std::string user_id = "cpp-client";
    std::string user_agent = "spark-connect-cpp";
    bool use_ssl = false;

    // Transport-level headers (Auth, Routing hints)
    std::map<std::string, std::string> headers;

    // Spark-level runtime configurations set via Builder.config()
    std::map<std::string, std::string> runtime_configs;

    Config();

    Config& setHost(std::string const& h);
    Config& setPort(int p);
    Config& setUserId(std::string const& u);
    Config& setUseSSL(bool ssl);
    Config& setHeader(std::string const& key, std::string const& value);

    /**
     * @brief An internal helper that stores Spark-level configs, set via
     * Builder.config().
     */
    Config& setRuntimeConfig(std::string const& key, std::string const& value);

    /**
     * @brief This sets Databricks specific headers
     * @param token Personal Access Token (PAT)
     * @param cluster_id The cluster ID (e.g., 1234-567890-abc123)
     */
    Config& setDatabricksAuth(std::string const& token,
                              std::string const& cluster_id);

    /**
     * @brief Sets Databricks Serverless specific headers
     * @param token Personal Access Token (PAT)
     * @param warehouse_id The SQL Warehouse ID (e.g., 12341a102456ee789)
     */
    Config& setServerlessAuth(std::string const& token,
                              std::string const& warehouse_id);

  private:
    static std::string generate_uuid();
};