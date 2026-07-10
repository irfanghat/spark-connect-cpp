#include <gmock/gmock-matchers.h>

#include "spark/spark_fixture.h"
#include "dataframe.h"
#include "ml/feature/stop_words_remover.h"
#include "ml/feature/tokenizer.h"

using ::testing::ElementsAre;

TEST_F(SparkIntegrationTest, StopWordsRemoverTransform)
{
    auto df = spark->sql(
        R"(
            SELECT *
            FROM
            VALUES
                (array('i', 'saw', 'the', 'red', 'balloon')),
                (array('mary', 'had', 'a', 'little', 'lamb'))
            AS sentences(raw)
        )");

    StopWordsRemover remover("raw", "filtered");

    auto transformed_df = remover.transform(df);
    auto result_df = transformed_df.select({"raw", "filtered"});

    result_df.show();

    auto rows = result_df.collect();

    ASSERT_EQ(rows.size(), 2);

    // Default English stop words drop "i", "the", "had", "a".
    EXPECT_THAT(rows[0].get<std::vector<std::string>>("filtered"),
                ElementsAre("saw", "red", "balloon"));
    EXPECT_THAT(rows[1].get<std::vector<std::string>>("filtered"),
                ElementsAre("mary", "little", "lamb"));
}

TEST_F(SparkIntegrationTest, StopWordsRemoverCustomStopWords)
{
    auto df = spark->sql(
        R"(
            SELECT *
            FROM
            VALUES
                (array('spark', 'connect', 'cpp', 'rocks'))
            AS sentences(raw)
        )");

    StopWordsRemover remover("raw", "filtered");
    remover.set_stop_words({"connect", "cpp"});

    auto transformed_df = remover.transform(df);
    auto rows = transformed_df.select({"filtered"}).collect();

    ASSERT_EQ(rows.size(), 1);
    EXPECT_THAT(rows[0].get<std::vector<std::string>>("filtered"),
                ElementsAre("spark", "rocks"));
}

TEST_F(SparkIntegrationTest, StopWordsRemoverChainedWithTokenizer)
{
    auto df = spark->sql(
        R"(
            SELECT *
            FROM
            VALUES
                ('I saw the red balloon')
            AS sentences(sentence)
        )");

    Tokenizer tokenizer("sentence", "words");
    StopWordsRemover remover("words", "filtered");

    auto tokenized_df = tokenizer.transform(df);
    auto filtered_df = remover.transform(tokenized_df);

    auto rows = filtered_df.select({"filtered"}).collect();

    ASSERT_EQ(rows.size(), 1);
    EXPECT_THAT(rows[0].get<std::vector<std::string>>("filtered"),
                ElementsAre("saw", "red", "balloon"));
}
