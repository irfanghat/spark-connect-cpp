#include "databricks/databricks_serverless_fixture.h"
#include "dataframe.h"
#include "ml/feature/word_2_vec.h"
#include "ml/feature/word_2_vec_model.h"

TEST_F(DatabricksServerlessIntegrationTest, Word2VecFit)
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

    EXPECT_FALSE(model.obj_ref().id().empty());
}
