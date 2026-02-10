#include <iostream>
#include <grpcpp/grpcpp.h>

#include "dataframe.h"
#include "reader.h"
#include "session.h"
#include "logging.h"

#include <spark/connect/relations.pb.h>
#include <spark/connect/commands.pb.h>

/**
 * @brief gRPC Metadata plugin to inject custom headers from the Spark Config
 * into every request sent by the stub. This is essential for Databricks
 * Authentication and cluster routing.
 */
class HeaderMetadataPlugin : public grpc::MetadataCredentialsPlugin
{
public:
    explicit HeaderMetadataPlugin(const std::map<std::string, std::string> &headers)
        : headers_(headers) {}

    grpc::Status GetMetadata(grpc::string_ref service_url,
                             grpc::string_ref method_name,
                             const grpc::AuthContext &auth_context,
                             std::multimap<grpc::string, grpc::string> *metadata) override
    {
        for (const auto &[key, value] : headers_)
        {
            metadata->insert(std::make_pair(key, value));
        }
        return grpc::Status::OK;
    }

private:
    std::map<std::string, std::string> headers_;
};

SparkSession *SparkSession::instance_ = nullptr;
std::once_flag SparkSession::once_flag_;

/**
 * @brief Private constructor for SparkSession.
 * @param config Configuration for the session.
 */
SparkSession::SparkSession(const Config &config)
    : config_(config)
{
    // --------------------------------------------------------
    // Build the gRPC channel and stub based on the config
    // --------------------------------------------------------
    std::string target = config_.host + ":" + std::to_string(config_.port);
    std::shared_ptr<grpc::ChannelCredentials> creds;

    if (config_.use_ssl)
    {
        // ---------------------------------------
        // For Databricks/Cloud, we enable SSL
        // ---------------------------------------
        auto ssl_creds = grpc::SslCredentials(grpc::SslCredentialsOptions());

        if (!config_.headers.empty())
        {
            // ----------------------------------------------------------------------------
            // If headers, such as Auth tokens exist, we create Composite Credentials
            // ----------------------------------------------------------------------------
            auto plugin = std::make_unique<HeaderMetadataPlugin>(config_.headers);
            auto call_creds = grpc::MetadataCredentialsFromPlugin(std::move(plugin));
            creds = grpc::CompositeChannelCredentials(ssl_creds, call_creds);
        }
        else
        {
            creds = ssl_creds;
        }
    }
    else
    {
        creds = grpc::InsecureChannelCredentials();
    }

    // --------------------------------------------------------
    // Apply Channel Arguments e.g. User Agent
    // --------------------------------------------------------
    grpc::ChannelArguments args;
    args.SetString(GRPC_ARG_PRIMARY_USER_AGENT_STRING, config_.user_agent);

    auto channel = grpc::CreateCustomChannel(target, creds, args);
    stub_ = spark::connect::SparkConnectService::NewStub(channel);
}

/**
 * @brief Gets or creates a new SparkSession with the specified configurations.
 * @return The singleton SparkSession instance.
 */
SparkSession &SparkSession::Builder::getOrCreate()
{
    std::call_once(SparkSession::once_flag_, [this]()
                   { SparkSession::instance_ = new SparkSession(this->config_); });
    return *SparkSession::instance_;
}

/**
 * @brief Returns a DataFrame representing the result of the given query.
 * @param query The SQL query string.
 * @return A new DataFrame instance.
 */
DataFrame SparkSession::sql(const std::string &query)
{
    spark::connect::Plan plan;
    plan.mutable_root()->mutable_sql()->set_query(query);
    return DataFrame(stub_, plan, config_.session_id, config_.user_id);
}

/**
 * @brief Generates a DataFrame containing a sequence of numbers.
 * * This is a leaf relation, meaning it generates data rather than
 * transforming existing data.
 *
 * @param start The first value in the sequence (inclusive).
 * @param end The boundary value (exclusive).
 * @param step The difference between consecutive numbers in the sequence.
 * @throw std::runtime_error If step is set to 0.
 * @return DataFrame containing a single column "id" of type Long.
 */
DataFrame SparkSession::range(int64_t start, int64_t end, int64_t step)
{
    spark::connect::Plan plan;
    auto *range_rel = plan.mutable_root()->mutable_range();

    range_rel->set_start(start);
    range_rel->set_end(end);
    range_rel->set_step(step);

    // --------------------------------------------------------------
    // Spark Connect default column name is usually "id"
    // We should support set partitions here in the future:
    // range_rel->set_num_partitions(2);
    // --------------------------------------------------------------

    return DataFrame(stub_, plan, config_.session_id, config_.user_id);
}

/**
 * @brief Overload for range starting at 0 with a step of 1.
 */
DataFrame SparkSession::range(int64_t end)
{
    return range(0, end, 1);
}

/**
 * @brief Creates a new SparkSession with an isolated session ID.
 * @return A new SparkSession instance.
 */
SparkSession SparkSession::newSession()
{
    SPARK_LOG_INFO("SparkSession", "Creating a new isolated session...");
    Config newConfig = this->config_;
    newConfig.session_id = "new_session_" + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                               std::chrono::system_clock::now().time_since_epoch())
                                                               .count());
    return SparkSession(newConfig);
}

/**
 * @brief Stops the underlying Spark session.
 */
void SparkSession::stop()
{
    // ------------------------------------------------
    // Build the request to release the session.
    // ------------------------------------------------
    spark::connect::ReleaseExecuteRequest request;
    request.set_session_id(config_.session_id);

    // ------------------------------------------------------
    // Create a new UserContext and set it in the request.
    // ------------------------------------------------------
    spark::connect::UserContext *user_context = request.mutable_user_context();
    user_context->set_user_id(config_.user_id);

    // ------------------------------------------------------
    // Set the release type to ReleaseAll.
    // ------------------------------------------------------
    request.mutable_release_all();

    // ------------------------------------------------------
    // Call the gRPC method and check the status.
    // ------------------------------------------------------
    grpc::ClientContext context;
    spark::connect::ReleaseExecuteResponse response;
    grpc::Status status = stub_->ReleaseExecute(&context, request, &response);

    if (status.ok())
    {
        SPARK_LOG_INFO("SparkSession", "SparkSession stopped successfully.");
    }
    else
    {
        SPARK_LOG_ERROR("SparkSession", "Failed to stop SparkSession: " + status.error_message());
    }
}

/**
 * @brief Returns a DataFrameReader that can be used to read data in as a DataFrame.
 * @return A new DataFrameReader instance.
 */
DataFrameReader SparkSession::read()
{
    return DataFrameReader(stub_, config_);
}