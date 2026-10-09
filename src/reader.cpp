#include "reader.h"

#include "dataframe.h"

#include <spark/connect/relations.pb.h>

/**
 * @brief Constructs a DataFrameReader.
 * @param stub A shared pointer to the gRPC stub.
 * @param config The session configuration.
 */
DataFrameReader::DataFrameReader(
    std::shared_ptr<spark::connect::SparkConnectService::Stub> stub,
    Config const& config)
    : stub_(stub), config_(config)
{
}

DataFrameReader&
DataFrameReader::format(std::string const& source)
{
    format_ = source;
    return *this;
}

DataFrameReader&
DataFrameReader::option(std::string const& key, std::string const& value)
{
    options_[key] = value;
    return *this;
}

DataFrameReader&
DataFrameReader::options(std::map<std::string, std::string> const& options)
{
    options_.insert(options.begin(), options.end());
    return *this;
}

DataFrameReader&
DataFrameReader::schema(std::string const& schema_ddl)
{
    schema_ddl_ = schema_ddl;
    return *this;
}

DataFrameReader&
DataFrameReader::schema(spark::sql::types::StructType const& schema_struct)
{
    schema_ddl_ = schema_struct.json();
    return *this;
}

DataFrame
DataFrameReader::load(std::vector<std::string> const& paths)
{
    spark::connect::Plan plan;

    auto* relation = plan.mutable_root();

    // -----------------------------------
    // Navigate the hierarchy
    // -----------------------------------
    auto* read = relation->mutable_read();
    auto* dataSource = read->mutable_data_source();

    dataSource->set_format(format_);

    if (!schema_ddl_.empty())
    {
        dataSource->set_schema(schema_ddl_);
    }

    for (auto const& path : paths)
    {
        dataSource->add_paths(path);
    }

    auto* options_map = dataSource->mutable_options();
    for (auto const& opt : options_)
    {
        (*options_map)[opt.first] = opt.second;
    }

    /**
     * @note Since spark->read() was called, config_ should be a copy from
     * the session Need a way to always ensure stub_ & config_ are valid
     */
    return DataFrame(stub_, plan, config_.session_id, config_.user_id);
}

DataFrame
DataFrameReader::csv(std::string const& path)
{
    return this->format("csv").load({path});
}

DataFrame
DataFrameReader::json(std::string const& path)
{
    return this->format("json").load({path});
}

DataFrame
DataFrameReader::text(std::string const& path)
{
    return this->format("text").load({path});
}

DataFrame
DataFrameReader::text(std::vector<std::string> const& paths)
{
    return this->format("text").load(paths);
}

DataFrame
DataFrameReader::parquet(std::string const& path)
{
    return this->format("parquet").load({path});
}

DataFrame
DataFrameReader::parquet(std::vector<std::string> const& paths)
{
    return this->format("parquet").load({paths});
}

DataFrame
DataFrameReader::orc(std::string const& path)
{
    return this->format("orc").load({path});
}

DataFrame
DataFrameReader::orc(std::vector<std::string> const& paths)
{
    return this->format("orc").load({paths});
}