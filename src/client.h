#pragma once

#include <grpcpp/grpcpp.h>
#include <spark/connect/base.grpc.pb.h>
#include <spark/connect/commands.pb.h>
#include <spark/connect/relations.pb.h>
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/memory.h>

#include "config.h"

class DataFrame
{
public:
    DataFrame(std::shared_ptr<spark::connect::SparkConnectService::Stub> stub,
              spark::connect::Plan plan,
              std::string session_id,
              std::string user_id)
        : stub_(std::move(stub)),
          plan_(std::move(plan)),
          session_id_(std::move(session_id)),
          user_id_(std::move(user_id)) {}

    void show(int max_rows = 10);

private:
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
    spark::connect::Plan plan_;
    std::string session_id_;
    std::string user_id_;
};

class SparkClient
{
public:
    explicit SparkClient(const Config &config)
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
