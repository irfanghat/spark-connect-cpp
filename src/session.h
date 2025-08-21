#pragma once

#include <grpcpp/grpcpp.h>
#include <spark/connect/base.grpc.pb.h>
#include "config.h"
#include "dataframe.h"

class SparkSession
{
public:
    explicit SparkSession(const Config &config)
        : config_(config)
    {
        std::string target = config_.host + ":" + std::to_string(config_.port);

        if (config_.use_ssl)
        {
            grpc::SslCredentialsOptions ssl_opts;
            stub_ = spark::connect::SparkConnectService::NewStub(
                grpc::CreateChannel(target, grpc::SslCredentials(ssl_opts)));
        }
        else
        {
            stub_ = spark::connect::SparkConnectService::NewStub(
                grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
        }
    }

    DataFrame sql(const std::string &query);
    DataFrame range(int64_t end);

    std::string session_id() const { return config_.session_id; }
    std::string user_id() const { return config_.user_id; }

private:
    Config config_;
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
};