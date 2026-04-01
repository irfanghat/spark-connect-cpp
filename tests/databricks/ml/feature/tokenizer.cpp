#include <gmock/gmock-matchers.h>

#include "../../databricks_serverless_fixture.h"
#include "dataframe.h"
#include "ml/feature/tokenizer.h"


using ::testing::ElementsAre;


TEST_F(DatabricksServerlessIntegrationTest, TokenizerTransform)
{
    auto df = spark->sql(
        R"(
            SELECT *
            FROM
            VALUES
                ('Hello World'),
                ('Spark Connect C++'),
                ('Tokenizer Test')
            AS sentences(sentence)
        )");

    Tokenizer tokenizer("sentence", "words");

    auto transformed_df = tokenizer.transform(df);
    auto result_df = transformed_df.select({ "sentence", "words" });
    
    result_df.show();

    auto rows = result_df.collect();

    ASSERT_EQ(rows.size(), 3);

    EXPECT_EQ(rows[0].get<std::string>("sentence"), "Hello World");
    EXPECT_THAT(rows[0].get<std::vector<std::string>>("words"), ElementsAre("hello", "world"));

    EXPECT_EQ(rows[1].get<std::string>("sentence"), "Spark Connect C++");
    EXPECT_THAT(rows[1].get<std::vector<std::string>>("words"), ElementsAre("spark", "connect", "c++"));

    EXPECT_EQ(rows[2].get<std::string>("sentence"), "Tokenizer Test");
    EXPECT_THAT(rows[2].get<std::vector<std::string>>("words"), ElementsAre("tokenizer", "test"));
}
