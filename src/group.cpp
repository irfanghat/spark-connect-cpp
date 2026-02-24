#include "group.h"

GroupedData::GroupedData(DataFrame &df, std::vector<spark::sql::types::Column> grouping_cols)
    : df_(df), grouping_cols_(grouping_cols) {}

/**
 * @brief An internal helper that wraps a `column name` in an `UnresolvedFunction`
 */
static spark::connect::Expression build_agg_expression(const std::string &func_name, const std::string &col_name)
{
    spark::connect::Expression e;
    auto *func = e.mutable_unresolved_function();
    func->set_function_name(func_name);
    func->add_arguments()->mutable_unresolved_attribute()->set_unparsed_identifier(col_name);
    return e;
}

DataFrame GroupedData::count()
{
    spark::connect::Plan plan;
    auto *agg = plan.mutable_root()->mutable_aggregate();

    // -------------------------------------------------------------------------------
    // We explicitly set the Group Type to GROUP_TYPE_GROUPBY.
    // If left as default (0), it is UNSPECIFIED and Spark will complain & reject the plan:
    // org.apache.spark.sql.connect.common.InvalidPlanInput: Unknown Group Type GROUP_TYPE_UNSPECIFIED
    // -------------------------------------------------------------------------------
    agg->set_group_type(spark::connect::Aggregate_GroupType_GROUP_TYPE_GROUPBY);
    agg->mutable_input()->CopyFrom(df_.plan_.root());

    for (const auto &col : grouping_cols_)
    {
        agg->add_grouping_expressions()->CopyFrom(*col.expr);
    }

    // ------------------------------------------------------------------------
    // Set aggregate expression, count(*)
    // Path: Aggregate -> AggregateExpression -> Alias -> UnresolvedFunction
    // ------------------------------------------------------------------------
    auto *agg_expr = agg->add_aggregate_expressions();
    auto *alias = agg_expr->mutable_alias();
    alias->add_name("count");
    auto *func = alias->mutable_expr()->mutable_unresolved_function();
    func->set_function_name("count");
    func->add_arguments()->mutable_unresolved_star();

    return DataFrame(df_.stub_, plan, df_.session_id_, df_.user_id_);
}

DataFrame GroupedData::sum(const std::vector<std::string> &cols)
{
    spark::connect::Plan plan;
    auto *agg = plan.mutable_root()->mutable_aggregate();
    agg->set_group_type(spark::connect::Aggregate_GroupType_GROUP_TYPE_GROUPBY);
    agg->mutable_input()->CopyFrom(df_.plan_.root());

    for (const auto &col : grouping_cols_)
    {
        agg->add_grouping_expressions()->CopyFrom(*col.expr);
    }

    for (const auto &col_name : cols)
    {
        agg->add_aggregate_expressions()->CopyFrom(build_agg_expression("sum", col_name));
    }

    return DataFrame(df_.stub_, plan, df_.session_id_, df_.user_id_);
}

DataFrame GroupedData::avg(const std::vector<std::string> &cols)
{
    spark::connect::Plan plan;
    auto *agg = plan.mutable_root()->mutable_aggregate();
    agg->set_group_type(spark::connect::Aggregate_GroupType_GROUP_TYPE_GROUPBY);
    agg->mutable_input()->CopyFrom(df_.plan_.root());

    for (const auto &col : grouping_cols_)
    {
        agg->add_grouping_expressions()->CopyFrom(*col.expr);
    }

    for (const auto &col_name : cols)
    {
        agg->add_aggregate_expressions()->CopyFrom(build_agg_expression("avg", col_name));
    }

    return DataFrame(df_.stub_, plan, df_.session_id_, df_.user_id_);
}