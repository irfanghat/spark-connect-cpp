#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "dataframe.h"

using namespace spark::connect;

class Word2VecModel;

class Word2Vec
{
  public:
    Word2Vec() = default;
    ~Word2Vec() = default;

    Word2VecModel fit(const DataFrame& input_df);

    void set_input_col(const std::string& input_col)
    {
        input_col_ = input_col;
    }

    void set_output_col(const std::string& output_col)
    {
        output_col_ = output_col;
    }

    void set_vector_size(int vector_size)
    {
        vector_size_ = vector_size;
    }

    void set_min_count(int min_count)
    {
        min_count_ = min_count;
    }

    void set_num_partitions(int num_partitions)
    {
        num_partitions_ = num_partitions;
    }

    void set_max_iter(int max_iter)
    {
        max_iter_ = max_iter;
    }
    void set_seed(int64_t seed)
    {
        seed_ = seed;
    }
    void set_window_size(int window_size)
    {
        window_size_ = window_size;
    }
    void set_max_sentence_length(int max_sentence_length)
    {
        max_sentence_length_ = max_sentence_length;
    }
    void set_step_size(double step_size)
    {
        step_size_ = step_size;
    }

    std::string input_col() const
    {
        return input_col_;
    }
    std::string output_col() const
    {
        return output_col_;
    }
    int vector_size() const
    {
        return vector_size_;
    }
    int min_count() const
    {
        return min_count_;
    }
    int num_partitions() const
    {
        return num_partitions_;
    }
    double step_size() const
    {
        return step_size_;
    }
    int max_iter() const
    {
        return max_iter_;
    }
    std::optional<int64_t> seed() const
    {
        return seed_;
    }
    int window_size() const
    {
        return window_size_;
    }
    int max_sentence_length() const
    {
        return max_sentence_length_;
    }

  private:
    const std::string class_name_ = "org.apache.spark.ml.feature.Word2Vec";
    const MlOperator_OperatorType operator_type_ =
        MlOperator_OperatorType_OPERATOR_TYPE_ESTIMATOR;

    std::string input_col_;
    std::string output_col_;
    std::optional<int64_t> seed_;

    int vector_size_ = 100;
    int min_count_ = 5;
    int num_partitions_ = 1;
    int max_iter_ = 1;
    int window_size_ = 5;
    int max_sentence_length_ = 1000;

    double step_size_ = 0.025;
};
