#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <iostream>
#include <sstream>
#include "session.h"
#include "config.h"
#include "dataframe.h"

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

    df.printSchema();

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

TEST_F(SparkIntegrationTest, SelectSubset)
{
    auto df = spark->sql(R"(SELECT 1 AS a, 2 AS b, 3 AS c)");

    // ------------------------------------------------------------
    // Select only 'b' and 'a' (Reordering them)
    // ------------------------------------------------------------
    auto df_subset = df.select({"b", "a"});

    // ------------------------------------------------------------
    // Verify column names and order
    // ------------------------------------------------------------
    EXPECT_THAT(df_subset.columns(), ElementsAre("b", "a"));

    // ------------------------------------------------------------
    // Verify show() still works on the new plan
    // ------------------------------------------------------------
    df_subset.show();
}

TEST_F(SparkIntegrationTest, Head)
{
    auto df = spark->sql("SELECT 1 AS a, 'Alice' AS b");

    auto rows = df.head(1);
    ASSERT_FALSE(rows.empty());
    const auto &row = rows[0];
    std::stringstream ss;
    ss << row;

    std::string expected = "Row(a=1, b='Alice')";
    EXPECT_EQ(ss.str(), expected);

    auto row_count = df.head().value().size();
    EXPECT_EQ(row_count, 2);

    // -----------------------------------------------------
    // Spark literals 1, 2, 3 are usually INT32 (int32_t),
    // therefore, 1L, would be INT64 (int64_t)
    // -----------------------------------------------------
    int32_t val_a = row.get<int32_t>("a");
    EXPECT_EQ(val_a, 1);

    // ----------------------------------------
    // String access
    // ----------------------------------------
    std::string val_b = row.get<std::string>("b");
    EXPECT_EQ(val_b, "Alice");
}

TEST_F(SparkIntegrationTest, First)
{
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv("datasets/people.csv");

    auto row = df.first();

    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->get<std::string>("name"), "John");
}

TEST_F(SparkIntegrationTest, DataFrameCount)
{
    auto df = spark->range(1000);
    df.show(1000);
    EXPECT_EQ(df.count(), 1000);
}

TEST_F(SparkIntegrationTest, Filter)
{
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv("datasets/people.csv");

    auto equal_to = df.filter("name = 'Josephine'");
    equal_to.show();

    // --------------------------------------------------------------
    // Numeric Comparison requires inferSchema to be set to "true",
    // otherwise, everything gets treated as a string
    // --------------------------------------------------------------
    auto greater_than = df.filter("age > 30");
    greater_than.show();

    // --------------------------------------------
    // Complex Logical Operators (AND, OR, NOT)
    // --------------------------------------------
    auto complex_logic = df.filter("age > 20 AND name LIKE 'J%' OR salary = 130000");
    complex_logic.show();

    // ----------------------------------------
    // Using SQL Functions inside Filter
    // ----------------------------------------
    auto sql_func_filter = df.filter("length(name) > 6");
    sql_func_filter.show();

    // ---------------------------
    // Filtering for NULLs
    // ---------------------------
    auto null_filter = df.filter("name IS NOT NULL");
    EXPECT_NO_THROW(null_filter.count());
    EXPECT_NE(null_filter.count(), 0);

    // ----------------------------------------------
    // Chained Filters via Nesting Doll pattern
    // ----------------------------------------------
    auto chained = df.filter("age > 18").filter("name != 'Unknown'");
    chained.show();
}

TEST_F(SparkIntegrationTest, WhereFilter)
{
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv("datasets/people.csv");

    auto filtered_df = df.where("age < 25");
    EXPECT_NO_THROW(filtered_df.show());
    EXPECT_LT(filtered_df.count(), df.count());
}

TEST_F(SparkIntegrationTest, DropDuplicates)
{
    auto df = spark->sql(R"(
        SELECT *
                FROM VALUES
                    (14, 'Tom'),
                    (14, 'Tom'),
                    (14, 'Alice'),
                    (14, 'Alice'),
                    (14, 'Bob'),
                    (14, 'Bob'),
                    (15, 'Tom'),
                    (15, 'John')
                AS people(age, name)
    )");

    auto deduped = df.dropDuplicates();
    deduped.show();

    EXPECT_EQ(deduped.count(), 5);

    auto subset_deduped = df.dropDuplicates({"age"});
    subset_deduped.show();

    EXPECT_EQ(subset_deduped.count(), 2);
}

TEST_F(SparkIntegrationTest, DataFrameCollect)
{
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv("datasets/people.csv");

    auto rows = df.collect();

    EXPECT_EQ(df.count(), rows.size());

    // --------------------------------------------
    // Check the first row: e.g., "John", 25
    // Refer to: /datasets/people.csv
    //
    // Sample: Row(name='John', age=25, salary=100000)
    // --------------------------------------------
    EXPECT_EQ(std::get<std::string>(rows[0]["name"]), "John");
    EXPECT_EQ(rows[0].get_long("age"), 25);

    // --------------------------------------------
    // Check the second row: e.g., "Andy", 30
    // Refer to: /datasets/people.csv
    //
    // Sample: Row(name='Alice', age=30, salary=85000)
    // --------------------------------------------
    EXPECT_EQ(std::get<std::string>(rows[1]["name"]), "Alice");
    EXPECT_EQ(rows[1].get_long("age"), 30);

    EXPECT_EQ(rows[0].column_names[0], "name");
    EXPECT_EQ(rows[0].column_names[1], "age");
    EXPECT_EQ(rows[0].column_names[2], "salary");

    auto cols = df.columns();
    EXPECT_EQ(cols.size(), 3);
    EXPECT_STREQ(cols[0].c_str(), "name");
}

TEST_F(SparkIntegrationTest, DataFrameDistinct)
{
    auto df = spark->sql(
        R"(
        SELECT * FROM
        VALUES 
            (1, "John"),
            (1, "John"),
            (2, "Sam"),
            (2, "Bob"),
            (3, "Anne")
        AS people(id, name)
    )");

    auto distinct_df = df.distinct();

    EXPECT_NO_THROW(distinct_df.show());
    EXPECT_GT(df.count(), distinct_df.count());
    EXPECT_EQ(distinct_df.count(), 4);

    auto rows = distinct_df.collect();
    std::set<std::string> unique_rows;

    for (const auto &row : rows)
    {
        std::string row_str = std::get<std::string>(row["name"]) + std::to_string(row.get_long("id"));
        unique_rows.insert(row_str);
    }

    EXPECT_EQ(unique_rows.size(), rows.size());
    EXPECT_EQ(rows.size(), 4);
}

TEST_F(SparkIntegrationTest, DataFrameDescribe)
{
    auto df = spark->sql(
        R"(
            SELECT * 
            FROM
            VALUES
                ("Bob", 13, 40.3, 150.5), 
                ("Alice", 12, 37.8, 142.3), 
                ("Tom", 11, 44.1, 142.2)
            AS people(name, age, weight, height)
        )");

    auto df_stats = df.describe({"age", "weight", "height"}).collect();

    EXPECT_EQ(std::get<std::string>(df_stats[0]["summary"]), "count");
    EXPECT_EQ(std::get<std::string>(df_stats[1]["summary"]), "mean");

    EXPECT_EQ(std::get<std::string>(df_stats[1]["age"]), "12.0");
}