#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <iostream>
#include "session.h"
#include "dataframe.h"
#include "config.h"
#include "functions.h"
#include "types.h"
#include "spark_fixture.h"

using namespace spark::sql::functions;
using namespace spark::sql::types;
using ::testing::ElementsAre;


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

    auto filtered_df = df.filter(col("name") == lit("Alice"))
                           .select({col("name"), _col.alias("age_plus_one")});

    filtered_df.show();

    auto rows = filtered_df.collect();

    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0].get<std::string>("name"), "Alice");

    // ---------------------------------------------------------------
    // Evaluate the _col expression result (23 + 1 = 24)
    // ---------------------------------------------------------------
    int64_t expected_age = 23 + 1;
    EXPECT_EQ(rows[0].get_long("age_plus_one"), expected_age);
}