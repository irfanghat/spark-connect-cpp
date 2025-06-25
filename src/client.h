#ifndef SPARK_CONNECT_CLIENT_H
#define SPARK_CONNECT_CLIENT_H

#include <memory>
#include <grpcpp/grpcpp.h>
#include "spark/connect/base.grpc.pb.h"

class SparkConnectClient
{
public:
    explicit SparkConnectClient(std::shared_ptr<grpc::Channel> channel);
    void SendTestQuery();

private:
    std::unique_ptr<spark::connect::SparkConnectService::Stub> stub_;
};

#endif
