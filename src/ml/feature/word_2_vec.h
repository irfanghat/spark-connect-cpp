#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "dataframe.h"
#include "ml/param/param_map.h"
#include "ml/param/param.h"

using namespace spark::connect;

class Word2VecModel;

class Word2Vec
{
    public:
        Word2Vec()
        : vector_size_(uid_, "vectorSize", "The dimension of the vector space. Default is 100."),
            min_count_(uid_, "minCount", "The minimum number of times a token must appear to be included in the word2vec model. Default is 5."),
            num_partitions_(uid_, "numPartitions", "The number of partitions to use for training. Default is 1."),
            max_iter_(uid_, "maxIter", "The maximum number of iterations (epochs) over the corpus. Default is 1."),
            window_size_(uid_, "windowSize", "The window size for context words. Default is 5."),
            max_sentence_length_(uid_, "maxSentenceLength", "The maximum length of each sentence. Default is 1000."),
            step_size_(uid_, "stepSize", "The step size (learning rate) for training. Default is 0.025."),
            seed_(uid_, "seed", "The random seed for initialization. Default is a random long integer."),
            input_column_(uid_, "inputCol", "The name of the input column. Default is \"\"."),
            output_column_(uid_, "outputCol", "The name of the output column. Default is \"\".")
        {}

        void set_input_col(const std::string& input_col)
        {
            params_.put(input_column_, input_col);
        }

        void set_output_col(const std::string& output_col)
        {
            params_.put(output_column_, output_col);
        }

        void set_vector_size(int vector_size)
        {
            params_.put(vector_size_, vector_size);
        }

        void set_min_count(int min_count)
        {
            params_.put(min_count_, min_count);
        }

        void set_num_partitions(int num_partitions)
        {
            params_.put(num_partitions_, num_partitions);
        }

        void set_max_iter(int max_iter)
        {
            params_.put(max_iter_, max_iter);
        }

        void set_seed(std::int64_t seed)
        {
            params_.put(seed_, seed);
        }

        void set_window_size(int window_size)
        {
            params_.put(window_size_, window_size);
        }

        void set_max_sentence_length(int max_sentence_length)
        {
            params_.put(max_sentence_length_, max_sentence_length);
        }

        void set_step_size(double step_size)
        {
            params_.put(step_size_, step_size);
        }

        std::string input_col() const
        {
            return params_.getOrElse(input_column_, std::string{});
        }

        std::string output_col() const
        {
            return params_.getOrElse(output_column_, std::string{});
        }

        int vector_size() const
        {
            return params_.getOrElse(vector_size_, 100);
        }

        int min_count() const
        {
            return params_.getOrElse(min_count_, 5);
        }

        int num_partitions() const
        {
            return params_.getOrElse(num_partitions_, 1);
        }

        double step_size() const
        {
            return params_.getOrElse(step_size_, 0.025);
        }

        int max_iter() const
        {
            return params_.getOrElse(max_iter_, 1);
        }

        std::optional<std::int64_t> seed() const
        {
            return params_.get(seed_);
        }

        int window_size() const
        {
            return params_.getOrElse(window_size_, 5);
        }

        int max_sentence_length() const
        {
            return params_.getOrElse(max_sentence_length_, 1000);
        }

        bool clear(const std::string& param_name)
        {
            if (param_name == "inputCol")
                return params_.remove(input_column_).has_value();

            if (param_name == "outputCol")
                return params_.remove(output_column_).has_value();

            if (param_name == "vectorSize")
                return params_.remove(vector_size_).has_value();

            if (param_name == "minCount")
                return params_.remove(min_count_).has_value();

            if (param_name == "numPartitions")
                return params_.remove(num_partitions_).has_value();

            if (param_name == "maxIter")
                return params_.remove(max_iter_).has_value();

            if (param_name == "seed")
                return params_.remove(seed_).has_value();

            if (param_name == "windowSize")
                return params_.remove(window_size_).has_value();

            if (param_name == "maxSentenceLength")
                return params_.remove(max_sentence_length_).has_value();

            if (param_name == "stepSize")
                return params_.remove(step_size_).has_value();

            return false;
        }

        Word2VecModel fit(const DataFrame& input_df);

    private:
        const std::string uid_ = "Word2Vec_" + std::to_string(reinterpret_cast<std::uintptr_t>(this));
        const std::string class_name_ = "org.apache.spark.ml.feature.Word2Vec";
        const MlOperator_OperatorType operator_type_ = MlOperator_OperatorType_OPERATOR_TYPE_ESTIMATOR;

        Param<std::string> input_column_;
        Param<std::string> output_column_;
        Param<int> vector_size_;
        Param<int> min_count_;
        Param<int> num_partitions_;
        Param<int> max_iter_;
        Param<int> window_size_;
        Param<int> max_sentence_length_;
        Param<std::int64_t> seed_;
        Param<double> step_size_;

        ParamMap params_;
};
