#pragma once

#include "config.h"
#include "session.h"

#include <map>
#include <memory>
#include <spark/connect/base.grpc.pb.h>
#include <spark/connect/commands.pb.h>
#include <spark/connect/relations.pb.h>
#include <string>
#include <vector>

/**
 * @class DataFrameWriter
 * @brief Interface used to write a `DataFrame` to external storage systems
 * (e.g. file systems, key-value stores, etc).
 */
class DataFrameWriter
{
  public:
    /**
     * @brief Constructs a DataFrameWriter.
     * @param stub A shared pointer to the gRPC stub.
     * @param config The session configuration.
     */
    DataFrameWriter(
        std::shared_ptr<spark::connect::SparkConnectService::Stub> stub,
        spark::connect::Relation const& relation, Config const& config);

    /**
     * @brief Specifies the behavior when data or table already exists.
     * Options include:
     *
     * - `append`: Append contents of this `DataFrame` to existing data
     *
     * - `overwrite`: Overwrite existing data.
     *
     * - `error` or `errorifexists`: Throw an exception if data already
     * exists.
     *
     * - `ignore`: Silently ignore this operation if data already exists.
     */
    DataFrameWriter& mode(std::string const& save_mode);

    /**
     * @brief Adds an output option for the underlying data source.
     * @param key The key for the option to set.
     * @param value The value for the option to set.
     */
    DataFrameWriter& option(std::string const& key, std::string const& value);

    /**
     * @brief Partitions the output by the given columns on the file system.
     * If specified, the output is laid out on the file system s
     * imilar to Hive's partitioning scheme.
     */
    DataFrameWriter& partitionBy(std::vector<std::string> const& cols);

    /**
     * @brief Specifies the underlying output data source.
     */
    DataFrameWriter& format(std::string const& source);

    /**
     * @brief Saves the content of the `DataFrame` in Parquet format at the
     * specified path.
     */
    void parquet(std::string const& path);

    /**
     * @brief Saves the content of the `DataFrame` in CSV format at the
     * specified path.
     */
    void csv(std::string const& path);

    /**
     * @brief Saves the content of the DataFrame in a text file at the
     * specified path. The text files will be encoded as UTF-8.
     */
    void text(std::string const& path);

    /**
     * @brief Saves the contents of the `DataFrame` to a data source.
     */
    void save(std::string const& path = "");

    /**
     * @brief Saves the content of the :class:`DataFrame` as the specified
     * table. In the case the table already exists, behavior of this
     * function depends on the save mode, specified by the `mode` function
     * (default to throwing an exception). When `mode` is `Overwrite`, the
     * schema of the :class:`DataFrame` does not need to be the same as that
     * of the existing table.
     *
     * - `append`: Append contents of this :class:`DataFrame` to existing
     * data.
     *
     * - `overwrite`: Overwrite existing data.
     *
     * - `error` or `errorifexists`: Throw an exception if data already
     * exists.
     *
     * - `ignore`: Silently ignore this operation if data already exists.
     *
     * @param tableName Name of the table.
     */
    void saveAsTable(std::string const& tableName);

  private:
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
    spark::connect::Relation input_relation_;
    Config config_;

    std::string format_;
    std::string mode_ = "errorifexists";
    std::map<std::string, std::string> options_;
    std::vector<std::string> partition_cols_;

    spark::connect::WriteOperation_SaveMode
    mapSaveMode(std::string const& mode);
};