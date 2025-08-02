#include <iostream>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <ctime>
#include <arrow/pretty_print.h>
#include "client.h"

using namespace spark::connect;

/**
 * @brief Executes a SQL query on the Spark backend and returns the result as a DataFrame.
 *
 * This function constructs a plan representing the SQL query,
 * serializes it, and sends it to the Spark backend using the client stub.
 *
 * @param query The SQL query to be executed.
 * @return DataFrame The resulting data from executing the query.
 *
 * @note This method requires an active session. The session ID and user ID
 *       are obtained from the client's configuration.
 *
 * @throws std::runtime_error if the execution fails on the backend.
 */
DataFrame SparkClient::sql(const std::string &query)
{
    Plan plan;
    plan.mutable_root()->mutable_sql()->set_query(query);
    return DataFrame(stub_, plan, config_.session_id, config_.user_id);
}

/**
 * @brief Generates a range of integers [0, end) on the Spark backend and returns it as a DataFrame.
 *
 * This method is typically used for generating synthetic data or testing pipelines.
 * It constructs a plan that describes a range operation.
 *
 * @param end The exclusive upper bound of the range.
 * @return DataFrame A DataFrame representing the range [0, end).
 *
 * @note This method also depends on an active session managed by the client.
 *
 * @throws std::runtime_error if the backend plan execution fails.
 */
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
    std::unique_ptr<grpc::ClientReader<ExecutePlanResponse>> reader(stub_->ExecutePlan(&context, request));

    ExecutePlanResponse response;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    while (reader->Read(&response))
    {
        if (response.has_arrow_batch())
        {
            auto arrow_buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t *>(response.arrow_batch().data().data()), response.arrow_batch().data().size());
            auto buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer);
            auto batch_reader = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader).ValueOrDie();

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

    //-----------------------------------------------------------------------
    // We only show the first batch for simplicity
    //-----------------------------------------------------------------------
    if (batches.empty())
    {
        std::cout << "Empty result" << std::endl;
        return;
    }

    auto batch = batches[0];
    int num_columns = batch->num_columns();
    int64_t num_rows = std::min(batch->num_rows(), static_cast<int64_t>(max_rows));

    std::vector<std::string> headers;
    std::vector<int> col_widths(num_columns, 12); // Default width

    for (int i = 0; i < num_columns; ++i)
    {
        headers.push_back(batch->schema()->field(i)->name());
    }

    //-----------------------------------------------------------------------
    // Header
    //-----------------------------------------------------------------------
    std::cout << "+";
    for (int i = 0; i < num_columns; ++i)
        std::cout << std::setw(col_widths[i]) << std::setfill('-') << "" << "+";

    std::cout << "\n|";
    for (int i = 0; i < num_columns; ++i)
        std::cout << std::setw(col_widths[i]) << std::setfill(' ') << std::left << headers[i] << "|";

    std::cout << "\n+";
    for (int i = 0; i < num_columns; ++i)
        std::cout << std::setw(col_widths[i]) << std::setfill('-') << "" << "+";

    std::cout << std::endl;

    //-----------------------------------------------------------------------
    // Rows
    //-----------------------------------------------------------------------
    for (int64_t row = 0; row < num_rows; ++row)
    {
        std::cout << "|";
        for (int col = 0; col < num_columns; ++col)
        {
            auto array = batch->column(col);
            std::string value;

            switch (array->type_id())
            {
            case arrow::Type::STRING:
            {
                auto str_array = std::static_pointer_cast<arrow::StringArray>(array);
                value = str_array->IsNull(row) ? "null" : str_array->GetString(row);
                break;
            }
            case arrow::Type::BOOL:
            {
                auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(array);
                value = bool_array->IsNull(row) ? "null" : (bool_array->Value(row) ? "true" : "false");
                break;
            }
            case arrow::Type::INT32:
            {
                auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
                value = int_array->IsNull(row) ? "null" : std::to_string(int_array->Value(row));
                break;
            }
            case arrow::Type::INT64:
            {
                auto int_array = std::static_pointer_cast<arrow::Int64Array>(array);
                value = int_array->IsNull(row) ? "null" : std::to_string(int_array->Value(row));
                break;
            }
            case arrow::Type::FLOAT:
            {
                auto float_array = std::static_pointer_cast<arrow::FloatArray>(array);
                value = float_array->IsNull(row) ? "null" : std::to_string(float_array->Value(row));
                break;
            }
            case arrow::Type::DOUBLE:
            {
                auto dbl_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                value = dbl_array->IsNull(row) ? "null" : std::to_string(dbl_array->Value(row));
                break;
            }
            case arrow::Type::DECIMAL128:
            {
                auto dec_array = std::static_pointer_cast<arrow::Decimal128Array>(array);
                if (dec_array->IsNull(row))
                {
                    value = "null";
                }
                else
                {
                    value = dec_array->FormatValue(row);
                }
                break;
            }
            case arrow::Type::DATE32:
            {
                auto date_array = std::static_pointer_cast<arrow::Date32Array>(array);
                if (date_array->IsNull(row))
                {
                    value = "null";
                }
                else
                {
                    int32_t days = date_array->Value(row);
                    std::chrono::system_clock::time_point tp = std::chrono::system_clock::from_time_t(0) + std::chrono::hours(days * 24);
                    std::time_t tt = std::chrono::system_clock::to_time_t(tp);
                    std::ostringstream oss;
                    oss << std::put_time(std::gmtime(&tt), "%Y-%m-%d");
                    value = oss.str();
                }
                break;
            }
            case arrow::Type::DATE64:
            {
                auto date_array = std::static_pointer_cast<arrow::Date64Array>(array);
                if (date_array->IsNull(row))
                {
                    value = "null";
                }
                else
                {
                    int64_t ms = date_array->Value(row);
                    std::chrono::milliseconds dur(ms);
                    std::chrono::system_clock::time_point tp(dur);
                    std::time_t tt = std::chrono::system_clock::to_time_t(tp);
                    std::ostringstream oss;
                    oss << std::put_time(std::gmtime(&tt), "%Y-%m-%d");
                    value = oss.str();
                }
                break;
            }
            case arrow::Type::TIMESTAMP:
            {
                auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(array);
                if (ts_array->IsNull(row))
                {
                    value = "null";
                }
                else
                {
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
                        value = "(unknown unit)";
                        break;
                    }

                    if (value.empty())
                    {
                        std::time_t tt = std::chrono::system_clock::to_time_t(tp);
                        std::ostringstream oss;
                        oss << std::put_time(std::gmtime(&tt), "%Y-%m-%d %H:%M:%S");
                        value = oss.str();
                    }
                }
                break;
            }
            default:
                value = "(unsupported)";
            }

            std::cout << std::setw(col_widths[col]) << std::setfill(' ') << std::left << value << "|";
        }
        std::cout << std::endl;
    }

    //-----------------------------------------------------------------------
    // Footer
    //-----------------------------------------------------------------------
    std::cout << "+";
    for (int i = 0; i < num_columns; ++i)
        std::cout << std::setw(col_widths[i]) << std::setfill('-') << "" << "+";
    std::cout << std::endl;
}
