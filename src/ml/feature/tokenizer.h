#pragma once

#include <spark/connect/relations.pb.h>

#include "../../dataframe.h"

using namespace spark::connect;


class Tokenizer
{
    public:

        Tokenizer(const std::string &input_col, const std::string &output_col)
            : input_col_(input_col), output_col_(output_col) {}

        ~Tokenizer() = default;

        void set_input_col(const std::string &input_col) {
            input_col_ = input_col;
        }

        void set_output_col(const std::string &output_col) {
            output_col_ = output_col;
        }

        std::string input_col() const {
            return input_col_;
        }

        std::string output_col() const {
            return output_col_;
        }

        DataFrame transform(const DataFrame &input_df);
        Relation transform(const Relation &input_relation);

    private:

        const std::string class_name_ = "org.apache.spark.ml.feature.Tokenizer";
        const MlOperator_OperatorType operator_type_ = MlOperator_OperatorType_OPERATOR_TYPE_TRANSFORMER;

        std::string input_col_;
        std::string output_col_;
};
