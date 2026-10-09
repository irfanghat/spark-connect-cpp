#pragma once

#include "dataframe.h"
#include "ml/param/param_map.h"

#include <spark/connect/relations.pb.h>

using namespace spark::connect;

/**
 * @brief A tokenizer that converts the input string to lowercase and then
 * splits it by white spaces.
 */
class Tokenizer
{
  public:
    Tokenizer(std::string const& input_col, std::string const& output_col)
    {
        set_input_col(input_col);
        set_output_col(output_col);
    }

    ~Tokenizer() = default;

    void
    set_input_col(std::string const& input_col)
    {
        params_.put("inputCol", input_col);
    }

    void
    set_output_col(std::string const& output_col)
    {
        params_.put("outputCol", output_col);
    }

    std::string
    input_col() const
    {
        auto value = params_.get("inputCol");

        if (!value.has_value())
            return "";

        auto const* col = std::get_if<std::string>(&value.value());

        return col ? *col : "";
    }

    std::string
    output_col() const
    {
        auto value = params_.get("outputCol");

        if (!value.has_value())
            return "";

        auto const* col = std::get_if<std::string>(&value.value());

        return col ? *col : "";
    }

    bool
    clear(std::string const& param_name)
    {
        if (params_.contains(param_name))
        {
            return params_.clear(param_name);
        }

        return false;
    }

    DataFrame transform(DataFrame const& input_df);
    Relation transform(Relation const& input_relation);

  private:
    std::string const class_name_ = "org.apache.spark.ml.feature.Tokenizer";
    MlOperator_OperatorType const operator_type_ =
        MlOperator_OperatorType_OPERATOR_TYPE_TRANSFORMER;

    ParamMap params_;
};
