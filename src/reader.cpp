#include "reader.h"
#include "dataframe.h"
#include <spark/connect/relations.pb.h>

/**
 * @brief Constructs a DataFrameReader.
 * @param stub A shared pointer to the gRPC stub.
 * @param config The session configuration.
 */
DataFrameReader::DataFrameReader(std::shared_ptr<spark::connect::SparkConnectService::Stub> stub,
                                 const Config &config)
    : stub_(stub), config_(config)
{
}

/**
 * @brief Specifies the input data source format.
 * @param source The name of the data source format (e.g., "csv", "json").
 * @return A reference to the DataFrameReader for chaining.
 */
DataFrameReader &DataFrameReader::format(const std::string &source)
{
    format_ = source;
    return *this;
}

/**
 * @brief Adds an input option for the underlying data source.
 * @param key The option key.
 * @param value The option value.
 * @return A reference to the DataFrameReader for chaining.
 */
DataFrameReader &DataFrameReader::option(const std::string &key, const std::string &value)
{
    options_[key] = value;
    return *this;
}

/**
 * @brief Adds multiple input options at once.
 * @param options A map of key-value pairs for the options.
 * @return A reference to the DataFrameReader for chaining.
 */
DataFrameReader &DataFrameReader::options(const std::map<std::string, std::string> &options)
{
    options_.insert(options.begin(), options.end());
    return *this;
}

/**
 * @brief Loads a DataFrame from the specified file or directory path.
 * @param paths A list of paths to the data files.
 * @return A new DataFrame instance.
 */
DataFrame DataFrameReader::load(const std::vector<std::string> &paths)
{
    spark::connect::Plan plan;

    auto *relation = plan.mutable_root();

    // -----------------------------------
    // Navigate the hierarchy
    // -----------------------------------
    auto *read = relation->mutable_read();
    auto *dataSource = read->mutable_data_source();

    dataSource->set_format(format_);

    for (const auto &path : paths)
    {
        dataSource->add_paths(path);
    }

    auto *options_map = dataSource->mutable_options();
    for (const auto &opt : options_)
    {
        (*options_map)[opt.first] = opt.second;
    }

    /**
     * @note Since spark->read() was called, config_ should be a copy from the session
     * Need a way to always ensure stub_ & config_ are valid
     */
    return DataFrame(stub_, plan, config_.session_id, config_.user_id);
}

/**
 * @brief Loads a CSV file into a DataFrame.
 * @param path The path to the CSV file.
 * @return A new DataFrame instance.
 */
DataFrame DataFrameReader::csv(const std::string &path)
{
    return this->format("csv").load({path});
}

/**
 * @brief Loads a JSON file into a DataFrame.
 * @param path The path to the JSON file.
 * @return A new DataFrame instance.
 */
DataFrame DataFrameReader::json(const std::string &path)
{
    return this->format("json").load({path});
}