#pragma once

#include <grpcpp/grpcpp.h>
#include <map>
#include <memory>
#include <optional>
#include <spark/connect/base.grpc.pb.h>
#include <string>
#include <vector>

/**
 * @class RuntimeConfig
 * @brief User-facing interface for reading and writing Spark session
 * configuration.
 */
class RuntimeConfig
{
  public:
    RuntimeConfig(
        std::shared_ptr<spark::connect::SparkConnectService::Stub> stub,
        std::string const& session_id, std::string const& user_id);

    /**
     * @brief Sets a config key to a string value.
     */
    void set(std::string const& key, std::string const& value);

    /**
     * @brief Sets a config key to a boolean value ("true"/"false").
     */
    void set(std::string const& key, bool value);

    /**
     * @brief Sets a config key to a long value.
     */
    void set(std::string const& key, int64_t value);

    /**
     * @brief Sets a config key to a string value.
     */
    void set(std::string const& key, char const* value);

    /**
     * @brief Gets a config value. Throws if the key is not set.
     */
    std::string get(std::string const& key) const;

    /**
     * @brief Gets a config value, returning a default if the key is not
     * set.
     */
    std::string get(std::string const& key,
                    std::string const& default_value) const;

    /**
     * @brief Gets a config value as an optional. Returns std::nullopt if
     * not set.
     */
    std::optional<std::string> getOption(std::string const& key) const;

    /**
     * @brief Returns all config key-value pairs visible to this session.
     */
    std::map<std::string, std::string> getAll() const;

    /**
     * @brief Unsets a config key, reverting it to its default.
     */
    void unset(std::string const& key);

    /**
     * @brief Returns true if the given key can be modified at runtime.
     */
    bool isModifiable(std::string const& key) const;

  private:
    spark::connect::ConfigResponse
    sendConfig(spark::connect::ConfigRequest::Operation const& op) const;

    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
    std::string session_id_;
    std::string user_id_;
};