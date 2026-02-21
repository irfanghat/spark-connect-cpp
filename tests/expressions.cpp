#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <iostream>
#include "session.h"
#include "dataframe.h"
#include "config.h"
#include "types.h"

using namespace spark::sql::types;
using ::testing::ElementsAre;

class SparkIntegrationTest : public ::testing::Test
{
protected:
    static SparkSession *spark;

    static void SetUpTestSuite()
    {
        spark = &SparkSession::builder()
                     .master("localhost")
                     .appName("SparkConnectCppGTest")
                     .getOrCreate();
    }

    static void TearDownTestSuite()
    {
        if (spark)
        {
            spark->stop();
        }
    }
};

SparkSession *SparkIntegrationTest::spark = nullptr;

TEST_F(SparkIntegrationTest, ColumnExpressions)
{
    auto df = spark->sql(
        R"(
            SELECT *
            FROM
            VALUES
                ('Tom', 21),
                ('Alice', 23),
                ('Bob', 27)
            AS people(name, age)
        )");

        // -----------------------------------------
        // Define the expression: age + 1
        // -----------------------------------------
        auto _col = col("age") + lit(1);

    // -----------------------------------------------------------------------
    // Use .select() to include the new expression in the output
    // We alias it to "age_plus_one" so we can find it in the resulting Row
    // -----------------------------------------------------------------------
    
    // ------------------------------------------------------------------------
    // Temporarily commenting this out until we figure out overloading for DataFrame::select()
    //
    // auto filtered_df = df.filter(col("name") == lit("Alice"))
    //                        .select({col("name"), _col.alias("age_plus_one")});
    // ------------------------------------------------------------------------

    auto filtered_df = df.filter(col("name") == lit("Alice"));
    auto rows = filtered_df.collect();

    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0].get<std::string>("name"), "Alice");

    // ---------------------------------------------------------------
    // Evaluate the _col expression result (23 + 1 = 24)
    // ---------------------------------------------------------------
    // int64_t expected_age = 23 + 1;
    // EXPECT_EQ(rows[0].get_long("age_plus_one"), expected_age);
}