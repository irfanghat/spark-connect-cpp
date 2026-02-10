#pragma once

#include <string>
#include <map>
#include <vector>

class Config
{
public:
    std::string host = "localhost";
    int port = 15002;
    std::string session_id;
    std::string user_id = "cpp-client";
    std::string user_agent = "spark-connect-cpp";
    bool use_ssl = false;
    std::map<std::string, std::string> headers;

    Config();

    Config &setHost(const std::string &h);
    Config &setPort(int p);
    Config &setUserId(const std::string &u);
    Config &setUseSSL(bool ssl);
    Config &setHeader(const std::string &key, const std::string &value);

    /**
     * @brief This sets Databricks specific headers
     * @param token Personal Access Token (PAT)
     * @param cluster_id The cluster ID (e.g., 1234-567890-abc123)
     */
    Config &setDatabricksAuth(const std::string &token, const std::string &cluster_id);

    /**
     * @brief Sets Databricks Serverless specific headers
     * @param token Personal Access Token (PAT)
     * @param warehouse_id The SQL Warehouse ID (e.g., 12341a102456ee789)
     */
    Config &setServerlessAuth(const std::string &token, const std::string &warehouse_id);

private:
    static std::string generate_uuid();
};