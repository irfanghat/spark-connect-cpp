#include <grpcpp/grpcpp.h>
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/memory.h>
#include <iostream>

#include "spark/connect/base.grpc.pb.h"
#include "spark/connect/commands.pb.h"
#include "spark/connect/relations.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using spark::connect::ExecutePlanRequest;
using spark::connect::ExecutePlanResponse;
using spark::connect::SparkConnectService;

int main()
{
    auto channel = grpc::CreateChannel("localhost:15002", grpc::InsecureChannelCredentials());
    std::unique_ptr<SparkConnectService::Stub> stub = SparkConnectService::NewStub(channel);

    ExecutePlanRequest request;
    request.set_session_id("00112233-4455-6677-8899-aabbccddeeff");
    request.mutable_user_context()->set_user_id("cpp-client");

    auto *plan = request.mutable_plan();
    auto *relation = plan->mutable_root();
    relation->mutable_sql()->set_query("SELECT id FROM range(10)");

    ClientContext context;
    std::unique_ptr<ClientReader<ExecutePlanResponse>> reader(stub->ExecutePlan(&context, request));

    ExecutePlanResponse response;
    while (reader->Read(&response))
    {
        if (response.has_arrow_batch())
        {
            std::cout << "[DEBUG] Rows: " << response.arrow_batch().row_count() << "\n";

            auto arrow_buffer = std::make_shared<arrow::Buffer>(
                reinterpret_cast<const uint8_t *>(response.arrow_batch().data().data()),
                response.arrow_batch().data().size());

            auto buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer);
            auto batch_reader = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader).ValueOrDie();

            while (true)
            {
                auto maybe_batch = batch_reader->Next();
                if (!maybe_batch.ok() || !maybe_batch.ValueOrDie())
                    break;

                auto batch = maybe_batch.ValueOrDie();
                std::cout << "[Arrow Schema] " << batch->schema()->ToString() << "\n";

                auto col_id = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
                for (int64_t i = 0; i < batch->num_rows(); i++)
                {
                    std::cout << "Row " << i << " -> id=" << col_id->Value(i) << "\n";
                }
            }
        }
    }

    grpc::Status status = reader->Finish();
    if (!status.ok())
    {
        std::cerr << "[ERROR] gRPC failed: " << status.error_message() << std::endl;
        return 1;
    }

    return 0;
}
