#include "writer.h"

DataFrameWriter::DataFrameWriter(
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub,
    spark::connect::Relation const& relation, Config const& config)
    : stub_(stub), config_(config)
{
    input_relation_.CopyFrom(relation);
}

DataFrameWriter&
DataFrameWriter::mode(std::string const& save_mode)
{
    mode_ = save_mode;
    return *this;
}

DataFrameWriter&
DataFrameWriter::format(std::string const& source)
{
    format_ = source;
    return *this;
}

DataFrameWriter&
DataFrameWriter::option(std::string const& key, std::string const& value)
{
    options_[key] = value;
    return *this;
}

DataFrameWriter&
DataFrameWriter::partitionBy(std::vector<std::string> const& cols)
{
    partition_cols_ = cols;
    return *this;
}

void
DataFrameWriter::parquet(std::string const& path)
{
    this->format("parquet").save(path);
}

void
DataFrameWriter::csv(std::string const& path)
{
    this->format("csv").save(path);
}

void
DataFrameWriter::text(std::string const& path)
{
    this->format("text").save(path);
}

void
DataFrameWriter::save(std::string const& path)
{
    spark::connect::Plan plan;

    auto* write_op = plan.mutable_command()->mutable_write_operation();

    // ---------------------------------------------------------------
    // Set data source relation (i.e., the DataFrame being written)
    // ---------------------------------------------------------------
    write_op->mutable_input()->CopyFrom(input_relation_);

    if (!format_.empty())
        write_op->set_source(format_);
    if (!path.empty())
        write_op->set_path(path);

    write_op->set_mode(mapSaveMode(mode_));

    auto* opts_map = write_op->mutable_options();
    for (auto const& [key, val] : options_)
    {
        (*opts_map)[key] = val;
    }

    for (auto const& col : partition_cols_)
    {
        write_op->add_partitioning_columns(col);
    }

    spark::connect::ExecutePlanRequest request;
    request.set_session_id(config_.session_id);
    request.mutable_user_context()->set_user_id(config_.user_id);
    *request.mutable_plan() = plan;

    grpc::ClientContext context;
    auto reader = stub_->ExecutePlan(&context, request);
    spark::connect::ExecutePlanResponse response;

    // --------------------------------------------------------------
    // NOTE: We need to consume the stream is required to trigger the action
    // --------------------------------------------------------------
    while (reader->Read(&response))
    {
        // -------------------------------------------------
        // We might need to process/handle Metrics here?
        // -------------------------------------------------
    }

    grpc::Status status = reader->Finish();
    if (!status.ok())
    {
        throw std::runtime_error("gRPC Write Error: " + status.error_message());
    }
}

spark::connect::WriteOperation_SaveMode
DataFrameWriter::mapSaveMode(std::string const& mode)
{
    if (mode == "overwrite")
        return spark::connect::WriteOperation_SaveMode_SAVE_MODE_OVERWRITE;
    if (mode == "append")
        return spark::connect::WriteOperation_SaveMode_SAVE_MODE_APPEND;
    if (mode == "ignore")
        return spark::connect::WriteOperation_SaveMode_SAVE_MODE_IGNORE;
    if (mode == "error" || mode == "errorifexists")
        return spark::connect::
            WriteOperation_SaveMode_SAVE_MODE_ERROR_IF_EXISTS;

    return spark::connect::WriteOperation_SaveMode_SAVE_MODE_UNSPECIFIED;
}