#pragma once

#include <string>
#include <vector>
#include <memory>
#include <map>
#include <spark/connect/base.grpc.pb.h>
#include <spark/connect/relations.pb.h>
#include "config.h"

class DataFrame;

/**
 * @class DataFrameReader
 * @brief Interface used to load a DataFrame from external storage systems.
 *
 * This class provides a fluent interface for configuring data source options
 * and loading data into a DataFrame.
 */
class DataFrameReader
{
public:
    /**
     * @brief Constructs a DataFrameReader.
     * @param stub A shared pointer to the gRPC stub.
     * @param config The session configuration.
     */
    DataFrameReader(std::shared_ptr<spark::connect::SparkConnectService::Stub> stub,
                    const Config &config);

    /**
     * @brief Specifies the input data source format.
     * @param source The name of the data source format (e.g., "csv", "json").
     * @return A reference to the DataFrameReader for chaining.
     */
    DataFrameReader &format(const std::string &source);

    /**
     * @brief Adds an input option for the underlying data source.
     * @param key The option key.
     * @param value The option value.
     * @return A reference to the DataFrameReader for chaining.
     */
    DataFrameReader &option(const std::string &key, const std::string &value);

    /**
     * @brief Adds multiple input options at once.
     * @param options A map of key-value pairs for the options.
     * @return A reference to the DataFrameReader for chaining.
     */
    DataFrameReader &options(const std::map<std::string, std::string> &options);

    /**
     * @brief Loads a DataFrame from the specified file or directory path.
     * @param paths A list of paths to the data files.
     * @return A new DataFrame instance.
     */
    DataFrame load(const std::vector<std::string> &paths);

    /**
     * @brief Loads a CSV file into a DataFrame.
     * @param path The path to the CSV file.
     * @return A new DataFrame instance.
     */
    DataFrame csv(const std::string &path);

    /**
     * @brief Loads a JSON file into a DataFrame.
     * @param path The path to the JSON file.
     * @return A new DataFrame instance.
     */
    DataFrame json(const std::string &path);

private:
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
    Config config_;
    std::string format_;
    std::map<std::string, std::string> options_;
};