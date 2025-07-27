#include "config.h"
#include <uuid/uuid.h>

Config::Config()
{
    session_id = generate_uuid();
}

Config &Config::setHost(const std::string &h)
{
    host = h;
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

std::string Config::generate_uuid()
{
    uuid_t uuid;
    uuid_generate(uuid);
    char str[37];
    uuid_unparse(uuid, str);
    return std::string(str);
}
