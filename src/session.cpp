#include "session.h"
#include "dataframe.h"
#include <spark/connect/relations.pb.h>
#include <spark/connect/commands.pb.h>

DataFrame SparkSession::sql(const std::string &query)
{
    spark::connect::Plan plan;
    plan.mutable_root()->mutable_sql()->set_query(query);
    return DataFrame(stub_, plan, config_.session_id, config_.user_id);
}

DataFrame SparkSession::range(int64_t end)
{
    spark::connect::Plan plan;
    plan.mutable_root()->mutable_range()->set_end(end);
    return DataFrame(stub_, plan, config_.session_id, config_.user_id);
}