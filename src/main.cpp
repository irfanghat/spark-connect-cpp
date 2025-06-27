#include <iostream>
#include <memory>
#include <string>

#include "spark/connect/base.grpc.pb.h" // For SparkConnectService::Stub
#include "spark/connect/base.pb.h"      // For common types like ExecutePlanRequest

#include <grpcpp/grpcpp.h>

int main() {
    std::cout << "Starting Spark Connect client test..." << std::endl;

    // --- Test Protobuf Message Creation ---
    // Create an instance of a Spark Connect protobuf message
    // This verifies that the .pb.h and .pb.cc files are correctly linked.
    spark::connect::ExecutePlanRequest request;
    request.set_session_id("test-session-id-123");
    std::cout << "Created an ExecutePlanRequest with session ID: " << request.session_id() << std::endl;

    // --- Test gRPC Stub Creation ---
    // This verifies that the .grpc.pb.h and .grpc.pb.cc files are correctly linked,
    // and that gRPC types are available.
    std::string target_str = "localhost:15002";
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials());

    std::unique_ptr<spark::connect::SparkConnectService::Stub> stub =
        spark::connect::SparkConnectService::NewStub(channel);

    if (stub) {
        std::cout << "Successfully created gRPC stub for SparkConnectService." << std::endl;
    } else {
        std::cerr << "Failed to create gRPC stub for SparkConnectService." << std::endl;
        return 1;
    }

    std::cout << "Spark Connect client test completed successfully." << std::endl;

    // Create gRPC call.
    // This example only tests the compilation and linking of the generated files.
    // For example:
    /*
    grpc::ClientContext context;
    spark::connect::ExecutePlanResponse response;
    grpc::Status status = stub->ExecutePlan(&context, request, &response);

    if (status.ok()) {
        std::cout << "gRPC call successful (though not actually connected to a server)." << std::endl;
    } else {
        std::cerr << "gRPC call failed: " << status.error_code() << ": " << status.error_message() << std::endl;
    }
    */

    return 0;
}