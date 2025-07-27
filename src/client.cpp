#include "client.h"
#include <arrow/pretty_print.h>
#include <iostream>

using namespace spark::connect;

DataFrame SparkClient::sql(const std::string &query)
{
    Plan plan;
    plan.mutable_root()->mutable_sql()->set_query(query);
    return DataFrame(stub_, plan, config_.session_id, config_.user_id);
}

DataFrame SparkClient::range(int64_t end)
{
    Plan plan;
    plan.mutable_root()->mutable_range()->set_end(end);
    return DataFrame(stub_, plan, config_.session_id, config_.user_id);
}

void DataFrame::show(int max_rows)
{
    ExecutePlanRequest request;
    request.set_session_id(session_id_);
    *request.mutable_plan() = plan_;
    request.mutable_user_context()->set_user_id(user_id_);

    grpc::ClientContext context;
    std::unique_ptr<grpc::ClientReader<ExecutePlanResponse>> reader(
        stub_->ExecutePlan(&context, request));

    ExecutePlanResponse response;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    while (reader->Read(&response))
    {
        if (response.has_arrow_batch())
        {
            auto arrow_buffer = std::make_shared<arrow::Buffer>(
                reinterpret_cast<const uint8_t *>(response.arrow_batch().data().data()),
                response.arrow_batch().data().size());

            auto buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer);
            auto batch_reader =
                arrow::ipc::RecordBatchStreamReader::Open(buffer_reader).ValueOrDie();

            while (true)
            {
                auto maybe_batch = batch_reader->Next();
                if (!maybe_batch.ok() || !maybe_batch.ValueOrDie())
                    break;

                batches.push_back(maybe_batch.ValueOrDie());
            }
        }
    }

    grpc::Status status = reader->Finish();
    if (!status.ok())
    {
        std::cerr << "[ERROR] gRPC failed: " << status.error_message() << std::endl;
        return;
    }

    arrow::PrettyPrintOptions options;
    options.indent = 0;
    options.window = max_rows;

    for (auto &batch : batches)
    {
        auto status = arrow::PrettyPrint(*batch, options, &std::cout);
        if (!status.ok())
        {
            std::cerr << "[ERROR] PrettyPrint failed: " << status.ToString() << std::endl;
        }
    }
}
