#include <algorithm>

#include "databricks/databricks_serverless_fixture.h"
#include "dataframe.h"
#include "ml/feature/word_2_vec.h"
#include "ml/feature/word_2_vec_model.h"

TEST_F(DatabricksServerlessIntegrationTest, Word2VecModelGetVectors)
{
    auto df = spark->sql(
        R"(
            SELECT *
            FROM
            VALUES
                (ARRAY('spark', 'connect', 'is', 'fast')),
                (ARRAY('word', 'to', 'vec', 'is', 'cool')),
                (ARRAY('cpp', 'client', 'for', 'spark'))
            AS sentences(words)
        )");

    Word2Vec word2vec;
    word2vec.set_input_col("words");
    word2vec.set_output_col("features");
    word2vec.set_vector_size(3);
    word2vec.set_min_count(1);

    auto model = word2vec.fit(df);
    auto vectors_df = model.getVectors();
    auto rows = vectors_df.collect();

    EXPECT_FALSE(rows.empty());

    auto columns = vectors_df.columns();
    EXPECT_NE(std::find(columns.begin(), columns.end(), "word"), columns.end());
    EXPECT_NE(std::find(columns.begin(), columns.end(), "vector"), columns.end());
}

TEST_F(DatabricksServerlessIntegrationTest, Word2VecModelGetVectorsShow)
{
    auto df = spark->sql(
        R"(
            SELECT *
            FROM
            VALUES
                (ARRAY('spark', 'connect', 'is', 'fast')),
                (ARRAY('word', 'to', 'vec', 'is', 'cool')),
                (ARRAY('cpp', 'client', 'for', 'spark'))
            AS sentences(words)
        )");

    Word2Vec word2vec;
    word2vec.set_input_col("words");
    word2vec.set_output_col("features");
    word2vec.set_vector_size(3);
    word2vec.set_min_count(1);

    auto model = word2vec.fit(df);
    auto vectors_df = model.getVectors();

    vectors_df.show();

    EXPECT_NO_THROW(vectors_df.show());
}

TEST_F(DatabricksServerlessIntegrationTest, Word2VecModelTransform)
{
    auto df = spark->sql(
        R"(
            SELECT *
            FROM
            VALUES
                (ARRAY('spark', 'connect', 'is', 'fast')),
                (ARRAY('word', 'to', 'vec', 'is', 'cool')),
                (ARRAY('cpp', 'client', 'for', 'spark'))
            AS sentences(words)
        )");

    Word2Vec word2vec;
    word2vec.set_input_col("words");
    word2vec.set_output_col("features");
    word2vec.set_vector_size(3);
    word2vec.set_min_count(1);

    auto model = word2vec.fit(df);
    auto transformed_df = model.transform(df);
    auto rows = transformed_df.collect();

    EXPECT_FALSE(rows.empty());
    EXPECT_EQ(rows.size(), 3);

    auto columns = transformed_df.columns();
    EXPECT_NE(std::find(columns.begin(), columns.end(), "words"), columns.end());
    EXPECT_NE(std::find(columns.begin(), columns.end(), "features"), columns.end());
}
