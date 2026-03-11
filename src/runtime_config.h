#pragma once

#include <string>
#include <vector>
#include <optional>
#include <memory>
#include <map>

#include <grpcpp/grpcpp.h>

#include <spark/connect/base.grpc.pb.h>

/**
 * @class RuntimeConfig
 * @brief User-facing interface for reading and writing Spark session configuration.
 */
class RuntimeConfig
{
public:
    RuntimeConfig(
        std::shared_ptr<spark::connect::SparkConnectService::Stub> stub,
        const std::string &session_id,
        const std::string &user_id);

    /**
     * @brief Sets a config key to a string value.
     */
    void set(const std::string &key, const std::string &value);

    /**
     * @brief Sets a config key to a boolean value ("true"/"false").
     */
    void set(const std::string &key, bool value);

    /**
     * @brief Sets a config key to a long value.
     */
    void set(const std::string &key, int64_t value);

    /**
     * @brief Gets a config value. Throws if the key is not set.
     */
    std::string get(const std::string &key) const;

    /**
     * @brief Gets a config value, returning a default if the key is not set.
     */
    std::string get(const std::string &key, const std::string &default_value) const;

    /**
     * @brief Gets a config value as an optional. Returns std::nullopt if not set.
     */
    std::optional<std::string> getOption(const std::string &key) const;

    /**
     * @brief Returns all config key-value pairs visible to this session.
     */
    std::map<std::string, std::string> getAll() const;

    /**
     * @brief Unsets a config key, reverting it to its default.
     */
    void unset(const std::string &key);

    /**
     * @brief Returns true if the given key can be modified at runtime.
     */
    bool isModifiable(const std::string &key) const;

private:
    spark::connect::ConfigResponse sendConfig(
        const spark::connect::ConfigRequest::Operation &op) const;

    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
    std::string session_id_;
    std::string user_id_;
};