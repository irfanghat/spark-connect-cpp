#pragma once

#include <string>
#include <map>
#include <uuid/uuid.h>

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

private:
    static std::string generate_uuid();
};
