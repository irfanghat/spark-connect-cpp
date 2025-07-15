#ifndef SPARK_CONNECT_CLIENT_H
#define SPARK_CONNECT_CLIENT_H

#include <string>
#include <memory>
#include <vector>

//--------------------------------------------------------------------------------
// gRPC and Protobuf includes
//--------------------------------------------------------------------------------
#include <grpcpp/grpcpp.h>
#include "spark/connect/base.grpc.pb.h"

//--------------------------------------------------------------------------------
// For spark::connect::Plan, UserContext, ExecutePlanResponse::ArrowBatch, etc.
//--------------------------------------------------------------------------------
#include "spark/connect/base.pb.h"

//-----------------------------------------------------------
// Apache Arrow includes
//-----------------------------------------------------------

//---------------------------------------------------------
// For std::shared_ptr<arrow::RecordBatch>
//-----------------------------------------------------------
#include <arrow/api.h>

//-------------------------------------------------------------------------------------------------------
// Since SparkConnectClient takes spark::connect::Plan, which internally uses
// relations.h/expressions.h types (via ToProto()), those headers are not strictly
// needed in client.h unless SparkConnectClient directly exposes methods returning/taking
// the custom SparkRelation/SparkExpression objects.
// For now, they are not directly used in the SparkConnectClient's public interface,
// so we don't need to include them here.
//-------------------------------------------------------------------------------------------------------

/**
 * @brief C++ Client for Spark Connect.
 *
 * This class handles communication with a Spark Connect server, allowing
 * execution of Spark plans and deserialization of Arrow data.
 */
class SparkConnectClient
{
public:
    /**
     * @brief Constructs a SparkConnectClient.
     * @param channel A shared pointer to a gRPC channel connected to the Spark Connect server.
     */
    SparkConnectClient(std::shared_ptr<grpc::Channel> channel);

    /**
     * @brief Executes a Spark Plan on the Spark Connect server.
     *
     * This method sends a Spark Plan, user context, and session ID to the server,
     * streams responses, and deserializes any Arrow batches received.
     *
     * @param plan The Spark Plan to execute.
     * @param user_context The user context for the session.
     * @param session_id The ID of the Spark session.
     * @return A vector of shared pointers to deserialized Arrow RecordBatches.
     */
    std::vector<std::shared_ptr<arrow::RecordBatch>> ExecutePlan(
        const spark::connect::Plan &plan,
        const spark::connect::UserContext &user_context,
        const std::string &session_id);

    // TODO: Add methods for AnalyzePlan, Config, etc.
    // Example:
    // void AnalyzePlan(const spark::connect::AnalyzePlanRequest& request);
    // void Config(const spark::connect::ConfigRequest& request);

private:
    //------------------------------------------
    // gRPC stub for the SparkConnectService
    //------------------------------------------
    std::unique_ptr<spark::connect::SparkConnectService::Stub> stub_;

    /**
     * @brief Helper function to deserialize an ArrowBatch received from Spark Connect.
     * @param arrow_batch The Protobuf ArrowBatch message containing serialized Arrow IPC data.
     * @return A shared pointer to the deserialized Arrow RecordBatch, or nullptr on failure.
     */
    std::shared_ptr<arrow::RecordBatch> DeserializeArrowBatch(
        const spark::connect::ExecutePlanResponse::ArrowBatch &arrow_batch);
};

#endif // SPARK_CONNECT_CLIENT_H
