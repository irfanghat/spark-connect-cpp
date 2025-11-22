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
static std::string arrayValueToString(std::shared_ptr<arrow::Array> array, int64_t row) {
    switch (array->type_id()) {
        case arrow::Type::STRING: {
            auto str_array = std::static_pointer_cast<arrow::StringArray>(array);
            return str_array->IsNull(row) ? "null" : str_array->GetString(row);
        }
        case arrow::Type::BOOL: {
            auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(array);
            return bool_array->IsNull(row) ? "null" : (bool_array->Value(row) ? "true" : "false");
        }
        case arrow::Type::INT32: {
            auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
            return int_array->IsNull(row) ? "null" : std::to_string(int_array->Value(row));
        }
        case arrow::Type::INT64: {
            auto int_array = std::static_pointer_cast<arrow::Int64Array>(array);
            return int_array->IsNull(row) ? "null" : std::to_string(int_array->Value(row));
        }
        case arrow::Type::FLOAT: {
            auto float_array = std::static_pointer_cast<arrow::FloatArray>(array);
            return float_array->IsNull(row) ? "null" : std::to_string(float_array->Value(row));
        }
        case arrow::Type::DOUBLE: {
            auto dbl_array = std::static_pointer_cast<arrow::DoubleArray>(array);
            return dbl_array->IsNull(row) ? "null" : std::to_string(dbl_array->Value(row));
        }
        case arrow::Type::DECIMAL128: {
            auto dec_array = std::static_pointer_cast<arrow::Decimal128Array>(array);
            return dec_array->IsNull(row) ? "null" : dec_array->FormatValue(row);
        }
        case arrow::Type::DATE32: {
            auto date_array = std::static_pointer_cast<arrow::Date32Array>(array);
            if (date_array->IsNull(row)) return "null";
            int32_t days = date_array->Value(row);
            std::chrono::system_clock::time_point tp = std::chrono::system_clock::from_time_t(0) + std::chrono::hours(days * 24);
            std::time_t tt = std::chrono::system_clock::to_time_t(tp);
            std::ostringstream oss;
            oss << std::put_time(std::gmtime(&tt), "%Y-%m-%d");
            return oss.str();
        }
        case arrow::Type::DATE64: {
            auto date_array = std::static_pointer_cast<arrow::Date64Array>(array);
            if (date_array->IsNull(row)) return "null";
            int64_t ms = date_array->Value(row);
            std::chrono::milliseconds dur(ms);
            std::chrono::system_clock::time_point tp(dur);
            std::time_t tt = std::chrono::system_clock::to_time_t(tp);
            std::ostringstream oss;
            oss << std::put_time(std::gmtime(&tt), "%Y-%m-%d");
            return oss.str();
        }
        case arrow::Type::TIMESTAMP: {
            auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(array);
            if (ts_array->IsNull(row)) return "null";
            
            int64_t ts = ts_array->Value(row);
            auto unit = std::static_pointer_cast<arrow::TimestampType>(ts_array->type())->unit();
            std::chrono::system_clock::time_point tp;

            switch (unit) {
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
                    tp = std::chrono::system_clock::time_point(
                        std::chrono::duration_cast<std::chrono::system_clock::duration>(
                            std::chrono::nanoseconds(ts)
                        )
                    );
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

/**
 * @brief Displays the contents of the DataFrame in a tabular format.
 *
 * This method prints the data returned from a Spark SQL query or transformation.
 * Internally, it deserializes the Arrow RecordBatch received from the Spark server
 * and formats it for terminal output.
 *
 * @param limit The maximum number of rows to display. If not provided, all rows are shown.
 *              Defaults to 10 if available.
 *
 * @note Supports pretty-printing of various data types including:
 *       - Integers and floating-point values
 *       - Strings
 *       - Booleans
 *       - Dates and timestamps (with formatting)
 *       - Nulls (displayed as "null")
 *
 * @example
 * SparkClient client(...);
 * auto df = client.sql("SELECT * FROM range(10)");
 * df.show(5);  // Display first 5 rows
 */
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
    std::vector<int> col_widths(num_columns);

    std::vector<std::vector<std::string>> string_data(num_rows, std::vector<std::string>(num_columns));
    
    for (int i = 0; i < num_columns; ++i)
    {
        headers.push_back(batch->schema()->field(i)->name());
        col_widths[i] = static_cast<int>(headers[i].length());
    }

    for (int64_t row = 0; row < num_rows; ++row) {
        for (int col = 0; col < num_columns; ++col) {
            std::string val = arrayValueToString(batch->column(col), row);
            string_data[row][col] = val;
            col_widths[col] = std::max(col_widths[col], static_cast<int>(val.length()));
        }
    }

    // Add padding (2 spaces)
    for (auto& w : col_widths) w += 2;

    // Separator line
    auto print_separator = [&]() {
        std::cout << "+";
        for (int w : col_widths)
            std::cout << std::string(w, '-') << "+";
        std::cout << std::endl;
    };

    //-----------------------------------------------------------------------
    // Header
    //-----------------------------------------------------------------------
    print_separator();
    std::cout << "|";
    for (int i = 0; i < num_columns; ++i)
        std::cout << " " << std::setfill(' ') << std::left << std::setw(col_widths[i] - 1) << headers[i] << "|";
    std::cout << std::endl;
    print_separator();

    //-----------------------------------------------------------------------
    // Rows
    //-----------------------------------------------------------------------
    for (int64_t row = 0; row < num_rows; ++row) {
        std::cout << "|";
        for (int col = 0; col < num_columns; ++col) {
            std::cout << " " << std::setfill(' ') << std::left << std::setw(col_widths[col] - 1) << string_data[row][col] << "|";
        }
        std::cout << std::endl;
        if(row < num_rows - 1)
            print_separator();
    }

    //-----------------------------------------------------------------------
    // Footer
    //-----------------------------------------------------------------------
    print_separator();

}
