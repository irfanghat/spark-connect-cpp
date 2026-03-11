#include "runtime_config.h"

#include <stdexcept>
#include <sstream>

RuntimeConfig::RuntimeConfig(
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub,
    const std::string &session_id,
    const std::string &user_id)
    : stub_(std::move(stub)), session_id_(session_id), user_id_(user_id)
{
}

spark::connect::ConfigResponse RuntimeConfig::sendConfig(
    const spark::connect::ConfigRequest::Operation &op) const
{
    spark::connect::ConfigRequest request;
    request.set_session_id(session_id_);
    request.mutable_user_context()->set_user_id(user_id_);
    *request.mutable_operation() = op;

    grpc::ClientContext context;
    spark::connect::ConfigResponse response;
    grpc::Status status = stub_->Config(&context, request, &response);

    if (!status.ok())
        throw std::runtime_error("Config RPC failed: " + status.error_message());

    return response;
}

void RuntimeConfig::set(const std::string &key, const std::string &value)
{
    spark::connect::ConfigRequest::Operation op;
    auto *kv = op.mutable_set()->add_pairs();
    kv->set_key(key);
    kv->set_value(value);
    sendConfig(op);
}

void RuntimeConfig::set(const std::string &key, bool value)
{
    set(key, value ? std::string("true") : std::string("false"));
}

void RuntimeConfig::set(const std::string &key, int64_t value)
{
    set(key, std::to_string(value));
}

void RuntimeConfig::set(const std::string &key, const char *value)
{
    set(key, std::string(value));
}

std::string RuntimeConfig::get(const std::string &key) const
{
    spark::connect::ConfigRequest::Operation op;
    op.mutable_get()->add_keys(key);
    auto response = sendConfig(op);

    if (response.pairs_size() == 0 || !response.pairs(0).has_value())
        throw std::runtime_error("Config key not found: " + key);

    return response.pairs(0).value();
}

std::string RuntimeConfig::get(const std::string &key,
                               const std::string &default_value) const
{
    spark::connect::ConfigRequest::Operation op;
    auto *gd = op.mutable_get_with_default()->add_pairs();
    gd->set_key(key);
    gd->set_value(default_value);
    auto response = sendConfig(op);

    if (response.pairs_size() == 0)
        return default_value;

    return response.pairs(0).has_value() ? response.pairs(0).value() : default_value;
}

std::optional<std::string> RuntimeConfig::getOption(const std::string &key) const
{
    spark::connect::ConfigRequest::Operation op;
    op.mutable_get_option()->add_keys(key);
    auto response = sendConfig(op);

    if (response.pairs_size() == 0 || !response.pairs(0).has_value())
        return std::nullopt;

    return response.pairs(0).value();
}

std::map<std::string, std::string> RuntimeConfig::getAll() const
{
    spark::connect::ConfigRequest::Operation op;

    // -------------------------------------------------
    // No arguments are needed, this returns all pairs
    // -------------------------------------------------
    op.mutable_get_all();
    auto response = sendConfig(op);

    std::map<std::string, std::string> result;
    for (const auto &pair : response.pairs())
    {
        if (pair.has_value())
            result[pair.key()] = pair.value();
        else
            result[pair.key()] = "";
    }
    return result;
}

void RuntimeConfig::unset(const std::string &key)
{
    spark::connect::ConfigRequest::Operation op;
    op.mutable_unset()->add_keys(key);
    sendConfig(op);
}

bool RuntimeConfig::isModifiable(const std::string &key) const
{
    spark::connect::ConfigRequest::Operation op;
    op.mutable_is_modifiable()->add_keys(key);
    auto response = sendConfig(op);

    if (response.pairs_size() == 0 || !response.pairs(0).has_value())
        return false;

    return response.pairs(0).value() == "true";
}