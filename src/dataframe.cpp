#include <iostream>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <ctime>

#include <arrow/pretty_print.h>
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/memory.h>

#include <grpcpp/grpcpp.h>
#include <spark/connect/base.grpc.pb.h>
#include <spark/connect/commands.pb.h>

#include "dataframe.h"
#include "types.h"

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

void DataFrame::show(int max_rows)
{
    ExecutePlanRequest request;
    request.set_session_id(session_id_);
    request.mutable_user_context()->set_user_id(user_id_);

    // -------------------------------------------------------------------
    // Use the existing limit() logic to create the correct Plan
    // This translate to something similar to the following behind the scenes:
    //
    // user_context {
    // user_id: "SparkConnectCppGTest"
    // }
    // plan {
    // root {
    //  lim...) (first 15 tasks are for partitions Vector(0))
    // -------------------------------------------------------------------
    if (max_rows > 0)
    {
        *request.mutable_plan() = this->limit(max_rows).plan_;
    }
    else
    {
        *request.mutable_plan() = plan_;
    }

    grpc::ClientContext context;
    // We might need a timeout in the future?
    // std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(30);
    // context.set_deadline(deadline);

    auto reader = stub_->ExecutePlan(&context, request);

    ExecutePlanResponse response;
    std::vector<std::vector<std::string>> string_data;
    std::vector<std::string> headers;
    std::vector<int> col_widths;

    // ---------------------------------------------------------------
    // Read the stream
    //
    // reader->Read() will return false if an error occurs
    // immediately (e.g. PATH_NOT_FOUND)
    // .. See: HandleMissingFile ..
    // ---------------------------------------------------------------
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

            if (headers.empty())
            {
                for (int i = 0; i < num_cols; ++i)
                {
                    headers.push_back(batch->schema()->field(i)->name());
                    col_widths.push_back(static_cast<int>(headers.back().length()));
                }
            }

            for (int64_t r = 0; r < batch->num_rows(); ++r)
            {
                std::vector<std::string> row_vec;
                for (int c = 0; c < num_cols; ++c)
                {
                    std::string val = arrayValueToString(batch->column(c), r);
                    row_vec.push_back(val);
                    if (c < (int)col_widths.size())
                    {
                        col_widths[c] = std::max(col_widths[c], (int)val.length());
                    }
                }
                string_data.push_back(std::move(row_vec));
            }
        }
    }

    // --------------------------------------------------------------------
    // Since we already applied the Limit in the Plan, we should read
    // the entire stream to let gRPC close naturally. Apache Spark's explain
    // feature can be useful when it comes to debugging the generated plans.
    //
    // See: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html
    // --------------------------------------------------------------------
    grpc::Status status = reader->Finish();
    if (!status.ok())
    {
        // ---------------------------------------------------------------
        // Throwing here allows GTest's EXPECT_THROW to catch the error
        // ---------------------------------------------------------------
        throw std::runtime_error("gRPC Error: [" + std::to_string(status.error_code()) + "] " + status.error_message());
    }

    if (string_data.empty())
    {
        std::cout << "++\n|| (Empty DataFrame)\n++" << std::endl;
        return;
    }

    // ---------------------------------------
    // Render the table
    // ---------------------------------------
    for (auto &w : col_widths)
        w += 2;
    auto print_sep = [&]()
    {
        std::cout << "+";
        for (int w : col_widths)
            std::cout << std::string(w, '-') << "+";
        std::cout << "\n";
    };

    print_sep();
    std::cout << "|";
    for (size_t i = 0; i < headers.size(); ++i)
    {
        std::cout << " " << std::left << std::setw(col_widths[i] - 1) << headers[i] << "|";
    }
    std::cout << "\n";
    print_sep();

    for (const auto &row : string_data)
    {
        std::cout << "|";
        for (size_t c = 0; c < row.size(); ++c)
        {
            std::cout << " " << std::left << std::setw(col_widths[c] - 1) << row[c] << "|";
        }
        std::cout << "\n";
    }
    print_sep();
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
    spark::connect::Plan new_plan;

    // ---------------------------------------------------------------------
    // Use the pointer to the root to ensure we are copying the content
    // ---------------------------------------------------------------------
    auto *project = new_plan.mutable_root()->mutable_project();

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

    return DataFrame(stub_, new_plan, session_id_, user_id_);
}

DataFrame DataFrame::limit(int n)
{
    spark::connect::Plan new_plan;
    auto *limit_rel = new_plan.mutable_root()->mutable_limit();
    *limit_rel->mutable_input() = this->plan_.root();
    limit_rel->set_limit(n);
    return DataFrame(stub_, new_plan, session_id_, user_id_);
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