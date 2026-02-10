#include "config.h"
#include <uuid/uuid.h>
#include <algorithm>

Config::Config()
{
    session_id = generate_uuid();
}

Config &Config::setHost(const std::string &h)
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

Config &Config::setPort(int p)
{
    port = p;
    return *this;
}

Config &Config::setUserId(const std::string &u)
{
    user_id = u;
    return *this;
}

Config &Config::setUseSSL(bool ssl)
{
    use_ssl = ssl;
    return *this;
}

Config &Config::setHeader(const std::string &key, const std::string &value)
{
    headers[key] = value;
    return *this;
}

Config &Config::setDatabricksAuth(const std::string &token, const std::string &cluster_id)
{
    this->setUseSSL(true);
    this->setPort(443);
    this->setHeader("authorization", "Bearer " + token);
    this->setHeader("x-databricks-cluster-id", cluster_id);
    return *this;
}

Config &Config::setServerlessAuth(const std::string &token, const std::string &warehouse_id)
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

std::string Config::generate_uuid()
{
    uuid_t uuid;
    uuid_generate(uuid);
    char str[37];
    uuid_unparse(uuid, str);
    return std::string(str);
}