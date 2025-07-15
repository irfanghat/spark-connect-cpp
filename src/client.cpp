#include "client.h"

#include <iostream>
#include <algorithm>

#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/buffer.h>

//---------------------------------------
// SparkConnectClient Constructor
//---------------------------------------
SparkConnectClient::SparkConnectClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(spark::connect::SparkConnectService::NewStub(channel)) {}

//-------------------------------------------------
// Helper function to deserialize an ArrowBatch
//-------------------------------------------------
std::shared_ptr<arrow::RecordBatch> SparkConnectClient::DeserializeArrowBatch(
    const spark::connect::ExecutePlanResponse::ArrowBatch &arrow_batch)
{
    if (arrow_batch.data().empty())
    {
        std::cerr << "Warning: Received empty ArrowBatch data." << std::endl;
        return nullptr;
    }

    //--------------------------------------------------------------------------------
    // An Arrow Buffer from the received data
    // The data() field of spark::connect::ExecutePlanResponse::ArrowBatch contains the serialized Arrow IPC message.
    //--------------------------------------------------------------------------------
    auto buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t *>(arrow_batch.data().data()),
        arrow_batch.data().size());

    //--------------------------------------------------------------------------------
    // A Shared pointer to BufferReader, as Arrow's Open methods often expect shared_ptr<InputStream>
    //--------------------------------------------------------------------------------
    auto buffer_reader_ptr = std::make_shared<arrow::io::BufferReader>(buffer);

    //--------------------------------------------------------------------------------
    // Create an IPC Reader. This is where we would typically read a RecordBatch.
    // Using RecordBatchStreamReader for potentially multiple RecordBatches within one stream
    // (though in Spark Connect, each ArrowBatch message seems to correspond to one RecordBatch)
    //--------------------------------------------------------------------------------
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>> reader_result =
        arrow::ipc::RecordBatchStreamReader::Open(buffer_reader_ptr);

    if (!reader_result.ok())
    {
        std::cerr << "Failed to open Arrow IPC stream reader: " << reader_result.status().ToString() << std::endl;
        return nullptr;
    }

    //--------------------------------------------------
    // Dereference the Result to get the shared_ptr
    //--------------------------------------------------
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader = *reader_result;

    std::shared_ptr<arrow::RecordBatch> record_batch;

    //--------------------------------------------------
    // Use the reader object to read
    //----------------------------------------------------
    arrow::Status status = reader->ReadNext(&record_batch);

    if (!status.ok())
    {
        std::cerr << "Failed to read Arrow RecordBatch: " << status.ToString() << std::endl;
        return nullptr;
    }

    if (!record_batch)
    {
        std::cerr << "No RecordBatch found in ArrowBatch (end of stream or empty batch)." << std::endl;
        return nullptr;
    }

    return record_batch;
}

//-----------------------------------------------
// ExecutePlan method implementation
//-----------------------------------------------
std::vector<std::shared_ptr<arrow::RecordBatch>> SparkConnectClient::ExecutePlan(
    const spark::connect::Plan &plan,
    const spark::connect::UserContext &user_context,
    const std::string &session_id)
{
    spark::connect::ExecutePlanRequest request;
    request.set_session_id(session_id);
    *request.mutable_user_context() = user_context;
    *request.mutable_plan() = plan;
    request.set_client_type("spark-connect-cpp-client");

    grpc::ClientContext context;
    std::unique_ptr<grpc::ClientReader<spark::connect::ExecutePlanResponse>> reader(
        stub_->ExecutePlan(&context, request));

    spark::connect::ExecutePlanResponse response;
    std::vector<std::shared_ptr<arrow::RecordBatch>> received_batches;

    while (reader->Read(&response))
    {
        std::cout << "Received response for session: " << response.session_id() << std::endl;

        //--------------------------------------------------------------------------------
        // Check if the response contains an ArrowBatch before attempting to access it.
        //--------------------------------------------------------------------------------
        if (response.has_arrow_batch())
        {
            std::cout << "  ArrowBatch: " << response.arrow_batch().row_count() << " rows" << std::endl;
            //--------------------------------------------------------------------------------
            // Deserialize the Arrow data here
            //--------------------------------------------------------------------------------
            std::shared_ptr<arrow::RecordBatch> batch = DeserializeArrowBatch(response.arrow_batch());
            if (batch)
            {
                received_batches.push_back(batch);
                std::cout << "  Successfully deserialized Arrow RecordBatch with "
                          << batch->num_rows() << " rows and "
                          << batch->num_columns() << " columns." << std::endl;
                //--------------------------------------------------------------------------------
                // TODO: Access data in the RecordBatch, e.g.:
                // std::cout << "  Schema: " << batch->schema()->ToString() << std::endl;
                // std::cout << "  First column type: " << batch->column(0)->type()->ToString() << std::endl;
                //--------------------------------------------------------------------------------
            }
        }
        if (response.has_schema())
        {
            std::cout << "  Schema: " << response.schema().DebugString() << std::endl;
        }
        if (response.has_metrics())
        {
            std::cout << "  Metrics received: " << response.metrics().metrics_size() << " objects" << std::endl;
        }
        //--------------------------------------------------------------------------------
        // TODO: Handle other response types here (e.g., sql_command_result, etc.)
        //--------------------------------------------------------------------------------
        if (response.has_sql_command_result())
        {
            std::cout << "  SQL Command Result: " << response.sql_command_result().DebugString() << std::endl;
        }
        if (response.has_streaming_query_command_result())
        {
            std::cout << "  Streaming Query Command Result: " << response.streaming_query_command_result().DebugString() << std::endl;
        }
        if (response.has_result_complete())
        {
            std::cout << "  Result Complete." << std::endl;
        }
    }

    grpc::Status status = reader->Finish();
    if (status.ok())
    {
        std::cout << "ExecutePlan RPC finished successfully." << std::endl;
    }
    else
    {
        std::cerr << "ExecutePlan RPC failed: " << status.error_code() << ": " << status.error_message() << std::endl;
    }

    return received_batches;
}
