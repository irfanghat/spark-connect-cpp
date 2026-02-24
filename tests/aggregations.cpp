#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gmock/gmock-matchers.h>
#include "session.h"
#include "config.h"
#include "dataframe.h"
#include "group.h"
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

TEST_F(SparkIntegrationTest, GroupingOperations)
{
    auto df = spark->sql(
        R"(
            SELECT * 
            FROM VALUES
                ('Alice', 21, 'Florida'),
                ('Bob', 30, 'Florida'),
                ('Tom', 25, 'Kansas')
            AS people(name, age, city)
        )");

    // ----------------------------------
    // Global Aggregation
    // ----------------------------------
    auto total_count = df.groupBy().count();
    total_count.show();

    auto row = *total_count.collect().data();

    EXPECT_EQ(row.get_long("count"), 3);

    // ----------------------------------
    // Simple Grouping
    // ----------------------------------
    auto city_counts = df.groupBy({"city"}).count();
    city_counts.show();

    auto cities = city_counts.collect();
    EXPECT_EQ(std::get<std::string>(cities[0]["city"]), "Florida");

    // --------------------------------------------------
    // Florida should have a count of 2 after grouping
    // --------------------------------------------------
    EXPECT_EQ(cities[0].get_long("count"), 2);

    EXPECT_EQ(std::get<std::string>(cities[1]["city"]), "Kansas");

    // --------------------------------------------------
    // Kansas should have a count of 1 after grouping
    // --------------------------------------------------
    EXPECT_EQ(cities[1].get_long("count"), 1);

    // ----------------------------------
    // Expression Grouping
    // ----------------------------------
    auto age_group_counts = df.groupBy({col("age") / lit(10)}).count();
    age_group_counts.show();

    auto age_groups = age_group_counts.collect();

    EXPECT_EQ(age_groups[0].get_double("(age / 10)"), 2.100000);
    EXPECT_EQ(age_groups[1].get_double("(age / 10)"), 3.000000);
    EXPECT_EQ(age_groups[2].get_double("(age / 10)"), 2.500000);
}