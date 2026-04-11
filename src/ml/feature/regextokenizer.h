#pragma once
#include <string>

#include <spark/connect/relations.pb.h>

#include "../../dataframe.h"

using namespace spark::connect;

class RegexTokenizer {
public:
    RegexTokenizer(const std::string& input_col,
                   const std::string& output_col,
                   const std::string& pattern = "\\s+")
        : input_col_(input_col), output_col_(output_col), pattern_(pattern) {}

                ~RegexTokenizer() = default;

        void set_input_col(const std::string &input_col) {
            input_col_ = input_col;
        }

        void set_output_col(const std::string &output_col) {
            output_col_ = output_col;
        }

        void set_pattern(const std::string &pattern) {
            pattern_ = pattern;
        }

        std::string input_col() const {
            return input_col_;
        }

        std::string output_col() const {
            return output_col_;
        }

        std::string pattern() const {
            return pattern_;
        }

    Relation transform(const Relation& input_relation);
    DataFrame transform(const DataFrame& input_df);

private:

    const std::string class_name_ = "org.apache.spark.ml.feature.RegexTokenizer";
    const MlOperator_OperatorType operator_type_ = MlOperator_OperatorType_OPERATOR_TYPE_TRANSFORMER;

    std::string input_col_;
    std::string output_col_;
    std::string pattern_;
};