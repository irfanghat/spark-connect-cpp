#include "config.h"

#include <algorithm>
#include <uuid/uuid.h>

Config::Config()
{
    session_id = generate_uuid();
}

Config&
Config::setHost(std::string const& h)
{
    std::string cleaned_host = h;

    // ------------------------------------------------
    // Databricks URLs often come as https://...
    // gRPC targets must not include the protocol.
    // ------------------------------------------------
    size_t pos = cleaned_host.find("://");
    if (pos != std::string::npos)
    {
        cleaned_host = cleaned_host.substr(pos + 3);
    }

    // ----------------------------------
    // Remove trailing slashes
    // ----------------------------------
    if (!cleaned_host.empty() && cleaned_host.back() == '/')
    {
        cleaned_host.pop_back();
    }

    host = cleaned_host;
    return *this;
}

Config&
Config::setPort(int p)
{
    port = p;
    return *this;
}

Config&
Config::setUserId(std::string const& u)
{
    user_id = u;
    return *this;
}

Config&
Config::setUseSSL(bool ssl)
{
    use_ssl = ssl;
    return *this;
}

Config&
Config::setHeader(std::string const& key, std::string const& value)
{
    headers[key] = value;
    return *this;
}

Config&
Config::setRuntimeConfig(std::string const& key, std::string const& value)
{
    runtime_configs[key] = value;
    return *this;
}

Config&
Config::setDatabricksAuth(std::string const& token,
                          std::string const& cluster_id)
{
    this->setUseSSL(true);
    this->setPort(443);
    this->setHeader("authorization", "Bearer " + token);
    this->setHeader("x-databricks-cluster-id", cluster_id);
    return *this;
}

Config&
Config::setServerlessAuth(std::string const& token,
                          std::string const& warehouse_id)
{
    this->setUseSSL(true);
    this->setPort(443);
    this->setHeader("authorization", "Bearer " + token);

    // ----------------------------------------------------------------------
    // Serverless needs the Session ID header (With Spark Connect)
    // We have to use the <warehouse_id> as the routing hint as well as
    // the internal <session_id>.
    // ----------------------------------------------------------------------
    this->setHeader("x-databricks-session-id", this->session_id);
    this->setHeader("x-databricks-sql-endpoint-id", warehouse_id);

    return *this;
}

std::string
Config::generate_uuid()
{
    uuid_t uuid;
    uuid_generate(uuid);
    char str[37];
    uuid_unparse(uuid, str);
    return std::string(str);
}