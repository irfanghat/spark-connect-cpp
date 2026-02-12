#include <iostream>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <ctime>

#include <arrow/pretty_print.h>
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/table.h>

#include <grpcpp/grpcpp.h>
#include <spark/connect/base.grpc.pb.h>
#include <spark/connect/commands.pb.h>

#include "dataframe.h"
#include "types.h"
#include "writer.h"

using namespace spark::connect;

DataFrame::DataFrame(std::shared_ptr<spark::connect::SparkConnectService::Stub> stub,
                     spark::connect::Plan plan,
                     std::string session_id,
                     std::string user_id)
    : stub_(stub),
      plan_(plan),
      session_id_(session_id),
      user_id_(user_id)
{
}

/**
 * @brief Converts a value from an Arrow Array at a specific row index to a string.
 *
 * This helper function handles the type-specific extraction of values from
 * Apache Arrow arrays. It supports various Arrow types and formats them
 * into human-readable strings for display purposes.
 *
 * @param array The shared pointer to the Arrow Array containing the data.
 * @param row The zero-based index of the row to extract.
 * @return A string representation of the value. Returns "null" if the value is null.
 *
 * @note Supported types:
 *       - STRING, BOOL
 *       - INT32, INT64
 *       - FLOAT, DOUBLE
 *       - DECIMAL128
 *       - DATE32, DATE64 (formatted as YYYY-MM-DD)
 *       - TIMESTAMP (formatted as YYYY-MM-DD HH:MM:SS)
 */
static std::string arrayValueToString(std::shared_ptr<arrow::Array> array, int64_t row)
{
    switch (array->type_id())
    {
    case arrow::Type::STRING:
    {
        auto str_array = std::static_pointer_cast<arrow::StringArray>(array);
        return str_array->IsNull(row) ? "null" : str_array->GetString(row);
    }
    case arrow::Type::BOOL:
    {
        auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(array);
        return bool_array->IsNull(row) ? "null" : (bool_array->Value(row) ? "true" : "false");
    }
    case arrow::Type::INT32:
    {
        auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
        return int_array->IsNull(row) ? "null" : std::to_string(int_array->Value(row));
    }
    case arrow::Type::INT64:
    {
        auto int_array = std::static_pointer_cast<arrow::Int64Array>(array);
        return int_array->IsNull(row) ? "null" : std::to_string(int_array->Value(row));
    }
    case arrow::Type::FLOAT:
    {
        auto float_array = std::static_pointer_cast<arrow::FloatArray>(array);
        return float_array->IsNull(row) ? "null" : std::to_string(float_array->Value(row));
    }
    case arrow::Type::DOUBLE:
    {
        auto dbl_array = std::static_pointer_cast<arrow::DoubleArray>(array);
        return dbl_array->IsNull(row) ? "null" : std::to_string(dbl_array->Value(row));
    }
    case arrow::Type::DECIMAL128:
    {
        auto dec_array = std::static_pointer_cast<arrow::Decimal128Array>(array);
        return dec_array->IsNull(row) ? "null" : dec_array->FormatValue(row);
    }
    case arrow::Type::DATE32:
    {
        auto date_array = std::static_pointer_cast<arrow::Date32Array>(array);
        if (date_array->IsNull(row))
            return "null";
        int32_t days = date_array->Value(row);
        std::chrono::system_clock::time_point tp = std::chrono::system_clock::from_time_t(0) + std::chrono::hours(days * 24);
        std::time_t tt = std::chrono::system_clock::to_time_t(tp);
        std::ostringstream oss;
        oss << std::put_time(std::gmtime(&tt), "%Y-%m-%d");
        return oss.str();
    }
    case arrow::Type::DATE64:
    {
        auto date_array = std::static_pointer_cast<arrow::Date64Array>(array);
        if (date_array->IsNull(row))
            return "null";
        int64_t ms = date_array->Value(row);
        std::chrono::milliseconds dur(ms);
        std::chrono::system_clock::time_point tp(dur);
        std::time_t tt = std::chrono::system_clock::to_time_t(tp);
        std::ostringstream oss;
        oss << std::put_time(std::gmtime(&tt), "%Y-%m-%d");
        return oss.str();
    }
    case arrow::Type::TIMESTAMP:
    {
        auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(array);
        if (ts_array->IsNull(row))
            return "null";

        int64_t ts = ts_array->Value(row);
        auto unit = std::static_pointer_cast<arrow::TimestampType>(ts_array->type())->unit();
        std::chrono::system_clock::time_point tp;

        switch (unit)
        {
        case arrow::TimeUnit::SECOND:
            tp = std::chrono::system_clock::time_point(std::chrono::seconds(ts));
            break;
        case arrow::TimeUnit::MILLI:
            tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(ts));
            break;
        case arrow::TimeUnit::MICRO:
            tp = std::chrono::system_clock::time_point(std::chrono::microseconds(ts));
            break;
        case arrow::TimeUnit::NANO:
            tp = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ts));
            break;
        default:
            return "(unknown unit)";
        }
        std::time_t tt = std::chrono::system_clock::to_time_t(tp);
        std::ostringstream oss;
        oss << std::put_time(std::gmtime(&tt), "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }
    default:
        return "(unsupported)";
    }
}

void DataFrame::show(int max_rows, int truncate)
{
    ExecutePlanRequest request;
    request.set_session_id(session_id_);
    request.mutable_user_context()->set_user_id(user_id_);

    if (max_rows > 0)
        *request.mutable_plan() = this->limit(max_rows).plan_;
    else
        *request.mutable_plan() = plan_;

    grpc::ClientContext context;
    auto reader = stub_->ExecutePlan(&context, request);
    ExecutePlanResponse response;

    bool headers_printed = false;
    int total_rows_printed = 0;
    std::vector<int> col_widths;
    std::ostringstream buffer; // Buffer output
    buffer << std::left;       // Set alignment once

    while (reader->Read(&response))
    {
        if (!response.has_arrow_batch())
            continue;

        auto arrow_buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t *>(response.arrow_batch().data().data()),
            response.arrow_batch().data().size());

        auto buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer);
        auto maybe_batch_reader = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
        if (!maybe_batch_reader.ok())
            continue;

        auto batch_reader = maybe_batch_reader.ValueOrDie();
        std::shared_ptr<arrow::RecordBatch> batch;

        while (batch_reader->ReadNext(&batch).ok() && batch)
        {
            int num_cols = batch->num_columns();
            int64_t num_rows = batch->num_rows();

            // ------------------------------------------------------
            // Determine Column Widths
            // ------------------------------------------------------
            if (col_widths.empty()) // First batch only
            {
                if (truncate > 0)
                {
                    // ----------------------------
                    // Fixed width mode
                    // ----------------------------
                    col_widths.assign(num_cols, truncate);
                }
                else
                {
                    // -------------------------------------------------------------
                    // Dynamic width mode
                    // Need to cache values for this batch
                    // -------------------------------------------------------------
                    col_widths.resize(num_cols, 0);

                    // ----------------------------
                    // Calculate from headers
                    // ----------------------------
                    for (int c = 0; c < num_cols; ++c)
                    {
                        col_widths[c] = batch->schema()->field(c)->name().length();
                    }
                }
            }

            // --------------------------------------------------
            // Cache string values to avoid double conversion
            // --------------------------------------------------
            std::vector<std::vector<std::string>> cached_values;
            // Only cache if we need dynamic widths
            if (truncate <= 0)
            {
                cached_values.resize(num_rows);
                for (int64_t r = 0; r < num_rows; ++r)
                {
                    cached_values[r].resize(num_cols);
                    for (int c = 0; c < num_cols; ++c)
                    {
                        cached_values[r][c] = arrayValueToString(batch->column(c), r);
                        col_widths[c] = std::max(col_widths[c], (int)cached_values[r][c].length());
                    }
                }
            }

            // ---------------------------------
            // Separator logic
            // ---------------------------------
            auto print_sep = [&]()
            {
                buffer << "+";
                for (int w : col_widths)
                    buffer << std::string(w + 2, '-') << "+";
                buffer << "\n";
            };

            // ------------------------------
            // Print Header
            // ------------------------------
            if (!headers_printed)
            {
                print_sep();
                buffer << "|";
                for (int i = 0; i < num_cols; ++i)
                {
                    buffer << " " << std::setw(col_widths[i])
                           << batch->schema()->field(i)->name() << " |";
                }
                buffer << "\n";
                print_sep();
                headers_printed = true;
            }

            // --------------------------------
            // Print Batch Rows
            // --------------------------------
            for (int64_t r = 0; r < num_rows; ++r)
            {
                // ----------------------------------------
                // Early termination if max_rows is set
                // ----------------------------------------
                if (max_rows > 0 && total_rows_printed >= max_rows)
                    goto finish;

                buffer << "|";
                for (int c = 0; c < num_cols; ++c)
                {
                    std::string val;
                    if (truncate <= 0 && !cached_values.empty())
                    {
                        // --------------------------------
                        // Use generated cache values
                        // --------------------------------
                        val = cached_values[r][c];
                    }
                    else
                    {
                        // ------------------------------------------------
                        // Convert types on demand for fixed width mode
                        // ------------------------------------------------
                        val = arrayValueToString(batch->column(c), r);
                        if (truncate > 0 && val.length() > (size_t)truncate)
                        {
                            val = val.substr(0, truncate - 3) + "...";
                        }
                    }

                    buffer << " " << std::setw(col_widths[c]) << val << " |";
                }
                buffer << "\n";
                total_rows_printed++;
            }
        }
    }

finish:
    if (total_rows_printed > 0)
    {
        buffer << "+";
        for (int w : col_widths)
            buffer << std::string(w + 2, '-') << "+";
        buffer << "\n";
    }
    else
    {
        buffer << "++\n|| (Empty DataFrame)\n++\n";
    }

    std::cout << buffer.str();

    grpc::Status status = reader->Finish();
    if (!status.ok())
        throw std::runtime_error("gRPC Error: " + status.error_message());
}

std::vector<std::string> DataFrame::columns() const
{
    std::vector<std::string> col_names;

    // ---------------------------------------------
    // Create AnalyzePlan request to get schema
    // ---------------------------------------------
    AnalyzePlanRequest request;
    request.set_session_id(session_id_);
    request.mutable_user_context()->set_user_id(user_id_);

    // ------------------------------
    // Set the schema analysis type
    // ------------------------------
    auto *schema_request = request.mutable_schema();
    *schema_request->mutable_plan() = plan_;

    // ------------------------------
    // Make the gRPC call
    // ------------------------------
    grpc::ClientContext context;
    AnalyzePlanResponse response;

    grpc::Status status = stub_->AnalyzePlan(&context, request, &response);

    if (!status.ok())
    {
        throw std::runtime_error("Failed to analyze plan for schema: " + status.error_message());
    }

    // ------------------------------------------
    // Extract column names from the schema
    // ------------------------------------------
    if (response.has_schema())
    {
        const auto &schema = response.schema().schema();
        if (schema.has_struct_())
        {
            for (const auto &field : schema.struct_().fields())
            {
                col_names.push_back(field.name());
            }
        }
        else
        {
            throw std::runtime_error("DataFrame Schema is not a struct type.");
        }
    }
    else
    {
        throw std::runtime_error("No schema found in AnalyzePlanResponse.");
    }
    return col_names;
}

spark::sql::types::StructType DataFrame::schema() const
{
    AnalyzePlanRequest request;
    request.set_session_id(session_id_);
    request.mutable_user_context()->set_user_id(user_id_);

    // ------------------------------
    // Set schema analysis type
    // ------------------------------
    auto *schema_request = request.mutable_schema();
    *schema_request->mutable_plan() = plan_;

    grpc::ClientContext context;
    AnalyzePlanResponse response;

    grpc::Status status = stub_->AnalyzePlan(&context, request, &response);

    if (!status.ok())
    {
        throw std::runtime_error("Failed to analyze plan for schema: " + status.error_message());
    }

    if (response.has_schema())
    {
        // -------------------------------------
        // Convert proto to C++ DataType
        // -------------------------------------
        spark::sql::types::DataType dt = spark::sql::types::DataType::from_proto(response.schema().schema());

        // ------------------------------------------------------------
        // A DataFrame schema is always a StructType at the root
        // ------------------------------------------------------------
        if (std::holds_alternative<spark::sql::types::StructType>(dt.kind))
        {
            return std::get<spark::sql::types::StructType>(dt.kind);
        }
        else
        {
            throw std::runtime_error("Internal Error: Spark returned a non-struct root schema.");
        }
    }
    else
    {
        throw std::runtime_error("No schema found in AnalyzePlanResponse.");
    }
}

void DataFrame::printSchema() const
{
    this->schema().print_tree(std::cout);
}

DataFrame DataFrame::select(const std::vector<std::string> &cols)
{
    spark::connect::Plan plan;

    // ---------------------------------------------------------------------
    // Use the pointer to the root to ensure we are copying the content
    // ---------------------------------------------------------------------
    auto *project = plan.mutable_root()->mutable_project();

    // ---------------------------------------------------------------------
    // Copy the entire relation tree from the previous plan
    // ---------------------------------------------------------------------
    if (this->plan_.has_root())
    {
        project->mutable_input()->CopyFrom(this->plan_.root());
    }

    for (const auto &col_name : cols)
    {
        auto *expr = project->add_expressions();
        expr->mutable_unresolved_attribute()->set_unparsed_identifier(col_name);
    }

    return DataFrame(stub_, plan, session_id_, user_id_);
}

DataFrame DataFrame::limit(int n)
{
    spark::connect::Plan plan;
    auto *limit_rel = plan.mutable_root()->mutable_limit();
    *limit_rel->mutable_input() = this->plan_.root();
    limit_rel->set_limit(n);
    return DataFrame(stub_, plan, session_id_, user_id_);
}

std::vector<spark::sql::types::Row> DataFrame::take(int n)
{
    ExecutePlanRequest request;
    request.set_session_id(session_id_);

    // ------------------------------------------------------------------
    // We use limit(n) to ensure we only pull necessary data over the wire
    // See note from PySpark API reference: https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.head.html#pyspark.sql.DataFrame.head
    // This method should only be used if the resulting array is expected
    // to be small, as all the data is loaded into the driver’s memory.
    // ------------------------------------------------------------------
    *request.mutable_plan() = this->limit(n).plan_;
    request.mutable_user_context()->set_user_id(user_id_);

    grpc::ClientContext context;
    auto reader = stub_->ExecutePlan(&context, request);
    ExecutePlanResponse response;
    std::vector<spark::sql::types::Row> result_rows;

    while (reader->Read(&response))
    {
        if (!response.has_arrow_batch())
            continue;

        // ---------------------------------------
        // Deserializing the Arrow Batch
        // ---------------------------------------
        auto buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t *>(response.arrow_batch().data().data()),
            response.arrow_batch().data().size());
        auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
        auto batch_reader = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader).ValueOrDie();

        std::shared_ptr<arrow::RecordBatch> batch;
        while (batch_reader->ReadNext(&batch).ok() && batch)
        {
            // ---------------------------------------
            // Extract column names once
            // ---------------------------------------
            std::vector<std::string> col_names;
            for (int i = 0; i < batch->num_columns(); ++i)
            {
                col_names.push_back(batch->schema()->field(i)->name());
            }

            for (int64_t i = 0; i < batch->num_rows(); ++i)
            {
                spark::sql::types::Row row;
                row.column_names = col_names;
                for (int col = 0; col < batch->num_columns(); ++col)
                {
                    row.values.push_back(spark::sql::types::arrayValueToVariant(batch->column(col), i));
                }
                result_rows.push_back(std::move(row));
                if (result_rows.size() >= static_cast<size_t>(n))
                    break;
            }
        }
    }
    return result_rows;
}

std::optional<spark::sql::types::Row> DataFrame::head()
{
    auto rows = take(1);
    return rows.empty() ? std::nullopt : std::make_optional(rows[0]);
}

std::vector<spark::sql::types::Row> DataFrame::head(int n)
{
    return take(n);
}

std::optional<spark::sql::types::Row> DataFrame::first()
{
    auto rows = take(1);
    return rows.empty() ? std::nullopt : std::make_optional(rows[0]);
}

int64_t DataFrame::count()
{
    // -----------------------------------------------------
    // Initialize the Plan and the Aggregate Relation
    // -----------------------------------------------------
    spark::connect::Plan count_plan;
    auto *aggregate = count_plan.mutable_root()->mutable_aggregate();

    // -------------------------------------------------------------------------------
    // We explicitly set the Group Type to GROUP_TYPE_GROUPBY.
    // If left as default (0), it is UNSPECIFIED and Spark will complain & reject the plan:
    // org.apache.spark.sql.connect.common.InvalidPlanInput: Unknown Group Type GROUP_TYPE_UNSPECIFIED
    // -------------------------------------------------------------------------------
    aggregate->set_group_type(spark::connect::Aggregate_GroupType_GROUP_TYPE_GROUPBY);

    // -----------------------------------------------------
    // Set the input to the current DataFrame's plan
    // -----------------------------------------------------
    *aggregate->mutable_input() = this->plan_.root();

    // ------------------------------------------------------------------------
    // Construct the "count(*)" aggregate expression
    // Path: Aggregate -> AggregateExpression -> Alias -> UnresolvedFunction
    // ------------------------------------------------------------------------
    auto *agg_expr = aggregate->add_aggregate_expressions();
    auto *alias = agg_expr->mutable_alias();

    // ----------------------------------------------------------------
    // Set Alias Name.
    // This is useful for identifying the column in the result batch
    // ----------------------------------------------------------------
    alias->add_name("count");

    // -------------------------------------------------------
    // Set function logic inside the alias expression
    // -------------------------------------------------------
    auto *func = alias->mutable_expr()->mutable_unresolved_function();
    func->set_function_name("count");

    // --------------------------------------------------
    // Add the "*" (star) argument to the function
    // --------------------------------------------------
    func->add_arguments()->mutable_unresolved_star();

    ExecutePlanRequest request;
    request.set_session_id(session_id_);
    request.mutable_user_context()->set_user_id(user_id_);
    *request.mutable_plan() = count_plan;

    grpc::ClientContext context;
    auto reader = stub_->ExecutePlan(&context, request);
    ExecutePlanResponse response;
    int64_t row_count = 0;

    while (reader->Read(&response))
    {
        if (!response.has_arrow_batch())
            continue;

        // -------------------------------------
        // Deserialize Arrow Batch
        // -------------------------------------
        auto buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t *>(response.arrow_batch().data().data()),
            response.arrow_batch().data().size());

        auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
        auto batch_reader = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader).ValueOrDie();

        std::shared_ptr<arrow::RecordBatch> batch;
        if (batch_reader->ReadNext(&batch).ok() && batch && batch->num_rows() > 0)
        {
            // -----------------------------------------------------------
            // Spark's count() result is in the first column (index 0)
            // -----------------------------------------------------------
            auto column = batch->column(0);

            // ------------------------------------------------------------
            // In Spark SQL, the result of a COUNT() aggregation is always
            // returned as a LongType (64‑bit integer), regardless of the size of the dataset.
            // ------------------------------------------------------------
            auto int_array = std::static_pointer_cast<arrow::Int64Array>(column);
            row_count = int_array->Value(0);
        }
    }

    grpc::Status status = reader->Finish();
    if (!status.ok())
    {
        throw std::runtime_error("gRPC Error: " + status.error_message());
    }

    return row_count;
}

DataFrame DataFrame::filter(const std::string &condition)
{
    spark::connect::Plan plan;

    // ------------------------------------------------------
    // Access the Filter relation in the new plan root
    // See: spark/connect/relations.proto (v3.5.x)
    // ------------------------------------------------------
    auto *filter_rel = plan.mutable_root()->mutable_filter();

    // ----------------------------------------------------------------------
    // Set input to the current relation (i.e. the parent of this filter)
    // ----------------------------------------------------------------------
    if (this->plan_.has_root())
    {
        filter_rel->mutable_input()->CopyFrom(this->plan_.root());
    }

    // ------------------------------------------------------------------
    // Set the condition using the ExpressionString message type
    //
    // In Spark 3.5, the plan is laid out as follows:
    // Filter -> Expression (condition) -> ExpressionString -> string (expression)
    // ------------------------------------------------------------------
    auto *condition_expr = filter_rel->mutable_condition();
    auto *expr_string_msg = condition_expr->mutable_expression_string();

    // -----------------------------------------------------------------
    // Set the actual SQL string inside the ExpressionString message
    // -----------------------------------------------------------------
    expr_string_msg->set_expression(condition);

    return DataFrame(stub_, plan, session_id_, user_id_);
}

DataFrame DataFrame::where(const std::string &condition)
{
    return filter(condition);
}

DataFrameWriter DataFrame::write()
{
    Config config;
    config.session_id = session_id_;
    config.user_id = user_id_;

    return DataFrameWriter(stub_, plan_.root(), config);
}

DataFrame DataFrame::dropDuplicates()
{
    return dropDuplicates({});
}

DataFrame DataFrame::dropDuplicates(const std::vector<std::string> &subset)
{
    spark::connect::Plan plan;

    auto *relation = plan.mutable_root()->mutable_deduplicate();

    if (this->plan_.has_root())
    {
        relation->mutable_input()->CopyFrom(this->plan_.root());
    }

    if (subset.empty())
    {
        relation->set_all_columns_as_keys(true);
    }
    else
    {
        for (const auto &col_name : subset)
        {
            relation->add_column_names(col_name);
        }
    }

    return DataFrame(stub_, plan, session_id_, user_id_);
}

DataFrame DataFrame::drop_duplicates()
{
    return dropDuplicates();
}

DataFrame DataFrame::drop_duplicates(const std::vector<std::string> &subset)
{
    return dropDuplicates(subset);
}

std::vector<spark::sql::types::Row> DataFrame::collect()
{
    std::vector<spark::sql::types::Row> results;

    spark::connect::ExecutePlanRequest request;
    request.set_session_id(session_id_);
    request.mutable_user_context()->set_user_id(user_id_);
    *request.mutable_plan() = plan_;

    grpc::ClientContext context;
    auto stream = stub_->ExecutePlan(&context, request);

    spark::connect::ExecutePlanResponse response;
    std::vector<std::string> col_names;
    bool schema_initialized = false;

    while (stream->Read(&response))
    {
        if (response.has_arrow_batch())
        {
            const auto &batch_proto = response.arrow_batch();

            auto buffer = std::make_shared<arrow::Buffer>(
                reinterpret_cast<const uint8_t *>(batch_proto.data().data()),
                batch_proto.data().size());
            arrow::io::BufferReader buffer_reader(buffer);

            auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(&buffer_reader);
            if (!reader_result.ok())
                continue;
            auto reader = reader_result.ValueOrDie();

            std::shared_ptr<arrow::RecordBatch> batch;
            while (reader->ReadNext(&batch).ok() && batch)
            {
                if (!schema_initialized)
                {
                    for (int i = 0; i < batch->num_columns(); ++i)
                    {
                        col_names.push_back(batch->column_name(i));
                    }
                    schema_initialized = true;
                }

                for (int64_t i = 0; i < batch->num_rows(); ++i)
                {
                    spark::sql::types::Row row;
                    row.column_names = col_names;

                    for (int j = 0; j < batch->num_columns(); ++j)
                    {
                        row.values.push_back(
                            spark::sql::types::arrayValueToVariant(batch->column(j), i));
                    }
                    results.push_back(std::move(row));
                }
            }
        }
    }

    auto status = stream->Finish();
    if (!status.ok())
    {
        throw std::runtime_error("gRPC Error during collect: " + status.error_message());
    }

    return results;
}