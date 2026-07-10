#pragma once

#include <spark/connect/relations.pb.h>

#include <string>
#include <vector>

#include "dataframe.h"
#include "ml/param/param_map.h"

using namespace spark::connect;

/**
 * @brief A feature transformer that filters out stop words from the input.
 *
 * Operates on a column of token arrays (typically the output of a Tokenizer)
 * and writes a new column with the stop words removed. When no custom stop
 * word list is set the server falls back to its default list for the locale.
 */
class StopWordsRemover
{
  public:
    StopWordsRemover(const std::string& input_col, const std::string& output_col)
    {
        set_input_col(input_col);
        set_output_col(output_col);
    }

    ~StopWordsRemover() = default;

    void set_input_col(const std::string& input_col)
    {
        params_.put("inputCol", input_col);
    }

    void set_output_col(const std::string& output_col)
    {
        params_.put("outputCol", output_col);
    }

    void set_stop_words(const std::vector<std::string>& stop_words)
    {
        params_.put("stopWords", stop_words);
    }

    void set_case_sensitive(bool case_sensitive)
    {
        params_.put("caseSensitive", case_sensitive);
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

    std::vector<std::string> stop_words() const
    {
        auto value = params_.get("stopWords");

        if (!value.has_value())
            return {};

        const auto* words = std::get_if<std::vector<std::string>>(&value.value());

        return words ? *words : std::vector<std::string>{};
    }

    bool case_sensitive() const
    {
        auto value = params_.get("caseSensitive");

        if (!value.has_value())
            return false;

        const auto* flag = std::get_if<bool>(&value.value());

        return flag ? *flag : false;
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
    const std::string class_name_ = "org.apache.spark.ml.feature.StopWordsRemover";
    const MlOperator_OperatorType operator_type_ =
        MlOperator_OperatorType_OPERATOR_TYPE_TRANSFORMER;

    ParamMap params_;
};
