#include <iostream>
#include <string>
#include <memory>
#include <vector>
#include <algorithm>

#include <grpcpp/grpcpp.h>
#include "client.h"

//-----------------------------------------------------------------
// Generated Protobuf messages (needed for Plan, UserContext)
//-----------------------------------------------------------------
#include "spark/connect/base.pb.h"

//-----------------------------------------------------------------
// SparkRelation and SparkExpression classes (needed for building the Plan)
//-----------------------------------------------------------------
#include "relations.h"
#include "expressions.h"

//-----------------------------------------------------------------
// Apache Arrow includes (needed for processing RecordBatches)
//-----------------------------------------------------------------
#include <arrow/api.h>

int main()
{
    //-------------------------------------------
    // Setup gRPC channel
    //-------------------------------------------
    std::string server_address("localhost:15002");

    //-------------------------------
    // TODO: Abstract channel creation
    //-------------------------------
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        server_address, grpc::InsecureChannelCredentials());

    SparkConnectClient client(channel);

    //--------------------------------------
    // Prepare UserContext and Session ID
    //--------------------------------------
    spark::connect::UserContext user_context;
    user_context.set_user_id("test_user_cpp");
    user_context.set_user_name("cpp_client_tester");

    //-----------------------------------------------------------------------------------------------------------
    // TODO: Generate a simple UUID for session_id (for testing, a fixed one is fine initially)
    // PLAN: Switch to a UUID library like Boost.UUID or a custom implementation.
    //-----------------------------------------------------------------------------------------------------------
    std::string session_id = "00112233-4455-6677-8899-aabbccddeeff";

    //------------------------------------------------------------------
    // Create a Spark Plan (via clinet.h classes)
    // Example: Create a RangeRelation
    //------------------------------------------------------------------
    std::shared_ptr<spark::client::RangeRelation> range_relation =
        spark::client::Range(10, std::optional<int64_t>{0}, std::optional<int64_t>{1}, std::optional<int32_t>{4});

    //------------------------------------------------------------------
    // Wrap the relation in a Plan message
    //------------------------------------------------------------------
    spark::connect::Plan plan;

    //--------------------------------------------------------
    // Convert C++ object to its Protobuf representation
    //--------------------------------------------------------
    *plan.mutable_root() = range_relation->ToProto();

    std::cout << "Executing plan:\n"
              << plan.DebugString() << std::endl;

    //--------------------------------------------------------
    // Execute the plan and get the Arrow RecordBatches
    //--------------------------------------------------------
    std::vector<std::shared_ptr<arrow::RecordBatch>> results =
        client.ExecutePlan(plan, user_context, session_id);

    //--------------------------------------------------------
    // Process the received Arrow RecordBatches
    //--------------------------------------------------------
    if (!results.empty())
    {
        std::cout << "\n[spark-connect-cpp]::[test] ==> Processing Received Arrow Data" << std::endl;
        for (size_t i = 0; i < results.size(); ++i)
        {
            const auto &batch = results[i];
            std::cout << "RecordBatch " << i + 1 << ": " << batch->num_rows() << " rows, "
                      << batch->num_columns() << " columns." << std::endl;
            std::cout << "  Schema: " << batch->schema()->ToString() << std::endl;

            //----------------------------------------------------------------
            // Example: Print the first few values of the first column
            //----------------------------------------------------------------
            if (batch->num_columns() > 0 && batch->num_rows() > 0)
            {
                std::shared_ptr<arrow::Array> first_column = batch->column(0);
                if (first_column->type_id() == arrow::Type::INT64)
                {
                    //----------------------------------------------------------------
                    // Assuming it's an int64 column from Range
                    //----------------------------------------------------------------
                    auto int64_array = std::static_pointer_cast<arrow::Int64Array>(first_column);
                    std::cout << "  First 5 values of first column: [";
                    for (int j = 0; j < std::min((int64_t)5, int64_array->length()); ++j)
                    {
                        if (j > 0)
                            std::cout << ", ";
                        std::cout << int64_array->Value(j);
                    }
                    std::cout << "]" << std::endl;
                }
                else
                {
                    std::cout << "  First column type is: " << first_column->type()->ToString() << std::endl;
                }
            }
        }
    }
    else
    {
        std::cout << "\n[spark-connect-cpp]::[test] ==> No Arrow RecordBatches received." << std::endl;
    }

    return 0;
}