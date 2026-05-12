#pragma once

#include <spark/connect/relations.pb.h>

#include "dataframe.h"
#include "ml/param/param_map.h"

using namespace spark::connect;

/**
 * @brief A tokenizer that converts the input string to lowercase and then
 * splits it by white spaces.
 */
class Tokenizer
{
  public:
    Tokenizer(const std::string& input_col, const std::string& output_col)
    {
                set_input_col(input_col);
                set_output_col(output_col);
    }

    ~Tokenizer() = default;

    void set_input_col(const std::string& input_col)
    {
        params_.put("inputCol", input_col);
    }

    void set_output_col(const std::string& output_col)
    {
        params_.put("outputCol", output_col);
    }

    std::string input_col() const
    {
        auto value = params_.get("inputCol");

        if (!value.has_value())
            return "";

        const auto* col = std::get_if<std::string>(&value.value());

        return col ? *col : "";
    }

    std::string output_col() const
    {
        auto value = params_.get("outputCol");

        if (!value.has_value())
            return "";

        const auto* col = std::get_if<std::string>(&value.value());
        
        return col ? *col : "";
    }

    bool clear(const std::string& param_name)
    {
        if (params_.contains(param_name))
        {
            return params_.clear(param_name);
        }

        return false;
    }

    DataFrame transform(const DataFrame& input_df);
    Relation transform(const Relation& input_relation);

  private:
    const std::string class_name_ = "org.apache.spark.ml.feature.Tokenizer";
    const MlOperator_OperatorType operator_type_ =
        MlOperator_OperatorType_OPERATOR_TYPE_TRANSFORMER;

    ParamMap params_;
};
