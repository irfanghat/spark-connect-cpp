#pragma once

#include <string>
#include <vector>
#include <memory>
#include <map>

#include <spark/connect/base.grpc.pb.h>
#include <spark/connect/commands.pb.h>
#include <spark/connect/relations.pb.h>

#include "config.h"
#include "session.h"

/**
 * @class DataFrameWriter
 * @brief Interface used to write a :class:`DataFrame` to external storage systems
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
    DataFrameWriter(std::shared_ptr<spark::connect::SparkConnectService::Stub> stub,
                    const spark::connect::Relation &relation,
                    const Config &config);

    DataFrameWriter &mode(const std::string &save_mode);
    DataFrameWriter &option(const std::string &key, const std::string &value);
    DataFrameWriter &partitionBy(const std::vector<std::string> &cols);
    DataFrameWriter &format(const std::string &source);

    void parquet(const std::string &path);
    void csv(const std::string &path);
    void save(const std::string &path = "");
    void saveAsTable(const std::string &tableName);

private:
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub_;
    spark::connect::Relation input_relation_;
    Config config_;

    std::string format_;
    std::string mode_ = "errorifexists";
    std::map<std::string, std::string> options_;
    std::vector<std::string> partition_cols_;

    spark::connect::WriteOperation_SaveMode mapSaveMode(const std::string &mode);
};