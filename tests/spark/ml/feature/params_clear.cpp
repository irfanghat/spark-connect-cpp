#include <gtest/gtest.h>

#include "ml/feature/tokenizer.h"
#include "ml/feature/word_2_vec.h"
#include "ml/param/param_map.h"

TEST(ParamMapTest, ClearRemovesParamWhenPresent)
{
    ParamMap params;
    params.put("inputCol", "sentence");

    EXPECT_TRUE(params.clear("inputCol"));
    EXPECT_FALSE(params.contains("inputCol"));
    EXPECT_TRUE(params.is_empty());
}

TEST(ParamMapTest, ClearReturnsFalseWhenParamAbsent)
{
    ParamMap params;

    EXPECT_FALSE(params.clear("missing"));
}

TEST(ParamMapTest, PutAllMergesAndOverwrites)
{
    ParamMap left;
    left.put("inputCol", "sentence");
    left.put("vectorSize", 100);

    ParamMap right;
    right.put("vectorSize", 200);
    right.put("minCount", 2);

    left.put_all(right);

    auto vector_size = left.get("vectorSize");
    ASSERT_TRUE(vector_size.has_value());
    ASSERT_NE(std::get_if<int>(&vector_size.value()), nullptr);
    EXPECT_EQ(*std::get_if<int>(&vector_size.value()), 200);

    EXPECT_TRUE(left.contains("inputCol"));
    EXPECT_TRUE(left.contains("minCount"));
}

TEST(ParamMapTest, ClearWithoutKeyRemovesAllEntries)
{
    ParamMap params;
    params.put("inputCol", "sentence");
    params.put("vectorSize", 100);

    params.clear();

    EXPECT_TRUE(params.is_empty());
}

TEST(TokenizerParamsTest, ClearResetsSupportedParams)
{
    Tokenizer tokenizer("sentence", "words");

    EXPECT_TRUE(tokenizer.clear("inputCol"));
    EXPECT_TRUE(tokenizer.clear("outputCol"));

    EXPECT_EQ(tokenizer.input_col(), "");
    EXPECT_EQ(tokenizer.output_col(), "");
}

TEST(TokenizerParamsTest, ClearReturnsFalseForUnknownParam)
{
    Tokenizer tokenizer("sentence", "words");

    EXPECT_FALSE(tokenizer.clear("unknown"));
}

TEST(Word2VecParamsTest, ClearResetsToDefaultsAndUnsetsOptional)
{
    Word2Vec word2vec;
    word2vec.set_input_col("words");
    word2vec.set_output_col("features");
    word2vec.set_vector_size(3);
    word2vec.set_min_count(1);
    word2vec.set_num_partitions(4);
    word2vec.set_step_size(0.1);
    word2vec.set_max_iter(9);
    word2vec.set_seed(42);
    word2vec.set_window_size(8);
    word2vec.set_max_sentence_length(256);

    EXPECT_TRUE(word2vec.clear("inputCol"));
    EXPECT_TRUE(word2vec.clear("outputCol"));
    EXPECT_TRUE(word2vec.clear("vectorSize"));
    EXPECT_TRUE(word2vec.clear("minCount"));
    EXPECT_TRUE(word2vec.clear("numPartitions"));
    EXPECT_TRUE(word2vec.clear("stepSize"));
    EXPECT_TRUE(word2vec.clear("maxIter"));
    EXPECT_TRUE(word2vec.clear("seed"));
    EXPECT_TRUE(word2vec.clear("windowSize"));
    EXPECT_TRUE(word2vec.clear("maxSentenceLength"));

    EXPECT_EQ(word2vec.input_col(), "");
    EXPECT_EQ(word2vec.output_col(), "");
    EXPECT_EQ(word2vec.vector_size(), 100);
    EXPECT_EQ(word2vec.min_count(), 5);
    EXPECT_EQ(word2vec.num_partitions(), 1);
    EXPECT_DOUBLE_EQ(word2vec.step_size(), 0.025);
    EXPECT_EQ(word2vec.max_iter(), 1);
    EXPECT_FALSE(word2vec.seed().has_value());
    EXPECT_EQ(word2vec.window_size(), 5);
    EXPECT_EQ(word2vec.max_sentence_length(), 1000);
}

TEST(Word2VecParamsTest, ClearReturnsFalseForUnknownParam)
{
    Word2Vec word2vec;

    EXPECT_FALSE(word2vec.clear("unknown"));
}
