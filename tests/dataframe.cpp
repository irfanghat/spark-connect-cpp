#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <iostream>
#include "session.h"
#include "config.h"
#include "dataframe.h"

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

// ----------------------------------------------------------------
// The following suite validates Basic Range Query logic,
// Simple String Selection, Mixed Types & Case logic,
// DateTime & Decimal processing/formatting, Date Ranges,
// Null & Boolean logic, Numeric Types &
// Binary formats, as well as Schema inspection.
// ----------------------------------------------------------------
TEST_F(SparkIntegrationTest, SessionIsActive)
{
    ASSERT_NE(spark, nullptr);
    EXPECT_FALSE(spark->session_id().empty());
    std::cout << "[INFO] Session ID: " << spark->session_id() << std::endl;
}

TEST_F(SparkIntegrationTest, BasicRangeQuery)
{
    auto df = spark->sql("SELECT * FROM range(10000000)");
    EXPECT_NO_THROW(df.show(5));
}

TEST_F(SparkIntegrationTest, SimpleStringSelection)
{
    auto df = spark->sql("SELECT 'John' AS name");
    EXPECT_NO_THROW(df.show());
}

TEST_F(SparkIntegrationTest, MixedTypesAndCaseLogic)
{
    auto df = spark->sql(R"(
        SELECT id, 
               CASE WHEN id % 2 = 0 THEN 'Alice' ELSE 'Bob' END AS name,
               id * 1.5 AS score,
               id % 3 = 0 AS is_active
        FROM range(10)
    )");
    EXPECT_NO_THROW(df.show(20));
}

TEST_F(SparkIntegrationTest, DateTimeAndDecimal)
{
    auto df = spark->sql(R"(
        SELECT 
            CAST('2024-01-01' AS DATE) AS date_col,
            CAST('2024-01-01 12:34:56' AS TIMESTAMP) AS ts_col,
            CAST(12345.6789 AS DECIMAL(20, 4)) AS decimal_col
    )");
    EXPECT_NO_THROW(df.show());
}

TEST_F(SparkIntegrationTest, DateRanges)
{
    auto df = spark->sql(R"(
        SELECT 
            CAST(date_sub(current_date(), CAST(id AS INT)) AS DATE) AS date32_col,
            CAST(date_add(current_timestamp(), CAST(id AS INT)) AS TIMESTAMP) AS ts_col
        FROM range(5)
    )");
    EXPECT_NO_THROW(df.show());
}

TEST_F(SparkIntegrationTest, NullAndBooleanLogic)
{
    auto df = spark->sql(R"(
        SELECT 
            IF(id % 2 = 0, null, id) AS maybe_null,
            id % 2 = 0 AS is_even
        FROM range(6)
    )");
    EXPECT_NO_THROW(df.show());
}

TEST_F(SparkIntegrationTest, NumericTypesAndBinary)
{
    auto df = spark->sql(R"(
        SELECT 
            CAST(id AS FLOAT) / 3.0 AS float_val,
            CAST(id AS DOUBLE) * 2.5 AS double_val,
            encode(CAST(id AS STRING), 'utf-8') AS bin_val
        FROM range(5)
    )");
    EXPECT_NO_THROW(df.show());
}

TEST_F(SparkIntegrationTest, SchemaIntrospection)
{
    auto df = spark->sql(R"(
        SELECT *
                FROM VALUES
                    (14, 'Tom'),
                    (14, 'Alice'),
                    (14, 'Bob')
                AS people(age, name)
    )");

    std::string schema_json = df.schema().json();

    std::string expected = "{\"type\":\"struct\",\"fields\":["
                           "{\"name\":\"age\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},"
                           "{\"name\":\"name\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}";

    EXPECT_EQ(schema_json, expected);

    std::cout << "StructType([" << schema_json << "])" << std::endl;
}

TEST_F(SparkIntegrationTest, AssertPrintSchema)
{
    // --------------------------------------------------------
    // Explicit casts to ensure the test is deterministic
    // --------------------------------------------------------
    auto df = spark->sql("SELECT CAST(1 AS INT) AS id, array(CAST(1.1 AS DOUBLE), CAST(2.2 AS DOUBLE)) AS vals");

    std::ostringstream oss;
    df.schema().print_tree(oss);
    std::string actual_tree = oss.str();

    // --------------------------------------------------------------------
    // The expected output matches Spark's specific tree formatting
    // Array nullability is on the field line
    // Indentation for nested elements uses 3 spaces after the pipe
    // --------------------------------------------------------------------
    std::string expected_tree =
        "root\n"
        " |-- id: integer (nullable = false)\n"
        " |-- vals: array\n"
        " |   |-- element: double (nullable = false)\n";

    EXPECT_EQ(actual_tree, expected_tree);
}