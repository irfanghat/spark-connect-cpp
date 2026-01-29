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

DataFrameReader &DataFrameReader::format(const std::string &source)
{
    format_ = source;
    return *this;
}

DataFrameReader &DataFrameReader::option(const std::string &key, const std::string &value)
{
    options_[key] = value;
    return *this;
}

DataFrameReader &DataFrameReader::options(const std::map<std::string, std::string> &options)
{
    options_.insert(options.begin(), options.end());
    return *this;
}

DataFrameReader &DataFrameReader::schema(const std::string &schema_ddl)
{
    schema_ddl_ = schema_ddl;
    return *this;
}

DataFrameReader &DataFrameReader::schema(const spark::sql::types::StructType &schema_struct)
{
    schema_ddl_ = schema_struct.json();
    return *this;
}

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

    if (!schema_ddl_.empty())
    {
        dataSource->set_schema(schema_ddl_);
    }

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

DataFrame DataFrameReader::csv(const std::string &path)
{
    return this->format("csv").load({path});
}

DataFrame DataFrameReader::json(const std::string &path)
{
    return this->format("json").load({path});
}

DataFrame DataFrameReader::text(const std::string &path)
{
    return this->format("text").load({path});
}

DataFrame DataFrameReader::text(const std::vector<std::string> &paths)
{
    return this->format("text").load(paths);
}

DataFrame DataFrameReader::parquet(const std::string &path)
{
    return this->format("parquet").load({path});
}

DataFrame DataFrameReader::parquet(const std::vector<std::string> &paths)
{
    return this->format("parquet").load({paths});
}