#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gmock/gmock-matchers.h>
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "session.h"
#include "dataframe.h"

using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::UnorderedElementsAre;

class ColumnTest : public ::testing::Test
{
protected:
    static SparkSession *spark;

    static void SetUpTestSuite()
    {
        spark = &SparkSession::builder()
                     .master("localhost")
                     .appName("DataFrameColumnsGTest")
                     .getOrCreate();
    }

    static void TearDownTestSuite()
    {
        if (spark)
            spark->stop();
    }
};

SparkSession *ColumnTest::spark = nullptr;

// ----------------------------------------------------------------
// The following suite validates Basic Column Retrieval (Single & Multiple),
// Column Order Preservation, Schema Preservation on empty DataFrames,
// Column Existence, Schema Comparison, Special Characters in Names,
// Column validation after Transformation, Error Handling (Invalid Plans),
// Column Data Type validation, as well as Complex Type detection.
// ----------------------------------------------------------------
TEST_F(ColumnTest, BasicColumnRetrieval)
{
    auto df = spark->sql("SELECT 14 AS age, 'Tom' AS name, 'CA' AS state");
    std::vector<std::string> expected = {"age", "name", "state"};

    EXPECT_THAT(df.columns(), ElementsAre("age", "name", "state"));
}

TEST_F(ColumnTest, ColumnOrderPreservation)
{
    auto df = spark->sql("SELECT 1 AS z, 2 AS a, 3 AS m");
    // ElementsAre checks for exact order
    EXPECT_THAT(df.columns(), ElementsAre("z", "a", "m"));
}

TEST_F(ColumnTest, SingleColumn)
{
    auto df = spark->sql("SELECT 42 AS answer");
    EXPECT_THAT(df.columns(), ElementsAre("answer"));
}

TEST_F(ColumnTest, ManyColumns)
{
    auto df = spark->sql(R"(
        SELECT 1 AS col1, 2 AS col2, 3 AS col3, 4 AS col4, 5 AS col5,
               6 AS col6, 7 AS col7, 8 AS col8, 9 AS col9, 10 AS col10
    )");
    auto cols = df.columns();
    ASSERT_EQ(cols.size(), 10);
    EXPECT_EQ(cols.front(), "col1");
    EXPECT_EQ(cols.back(), "col10");
}

TEST_F(ColumnTest, EmptyDataFramePreservesSchema)
{
    auto df = spark->sql("SELECT 1 AS id, 'test' AS name WHERE 1=0");
    EXPECT_THAT(df.columns(), ElementsAre("id", "name"));
}

TEST_F(ColumnTest, ColumnExistenceCheck)
{
    auto df = spark->sql("SELECT 1 AS id, 'John' AS name, 100 AS salary");
    auto cols = df.columns();

    EXPECT_TRUE(std::find(cols.begin(), cols.end(), "salary") != cols.end());
    EXPECT_FALSE(std::find(cols.begin(), cols.end(), "age") != cols.end());
}

TEST_F(ColumnTest, SchemaComparison)
{
    auto df1 = spark->sql("SELECT 1 AS id, 'Alice' AS name");
    auto df2 = spark->sql("SELECT 2 AS id, 'Bob' AS name");
    auto df3 = spark->sql("SELECT 3 AS id, 'Charlie' AS username");

    EXPECT_EQ(df1.columns(), df2.columns());
    EXPECT_NE(df1.columns(), df3.columns());
}

TEST_F(ColumnTest, SpecialCharactersInNames)
{
    auto df = spark->sql("SELECT 1 AS `user.id`, 2 AS `count(*)`, 3 AS `col with spaces`");
    EXPECT_THAT(df.columns(), ElementsAre("user.id", "count(*)", "col with spaces"));
}

TEST_F(ColumnTest, ColumnsAfterTransformation)
{
    auto df_selected = spark->sql("SELECT b, a FROM (SELECT 1 AS a, 2 AS b, 3 AS c)");
    EXPECT_THAT(df_selected.columns(), ElementsAre("b", "a"));
}

TEST_F(ColumnTest, ErrorHandlingInvalidPlan)
{
    // gTest will catch the exception. We assert that it MUST throw.
    EXPECT_THROW({
        auto df = spark->sql("SELECT * FROM non_existent_table_12345");
        auto cols = df.columns(); }, std::exception);
}

TEST_F(ColumnTest, ColumnDataTypesValidation)
{
    auto df = spark->sql(R"(
        SELECT 
            CAST(1 AS INT) AS int_col,
            CAST('hello' AS STRING) AS str_col,
            CAST(3.14 AS DOUBLE) AS double_col,
            CAST('2024-01-01' AS DATE) AS date_col,
            CAST(1 = 1 AS BOOLEAN) AS bool_col
    )");

    std::string schema_str = df.schema().json();

    EXPECT_THAT(schema_str, HasSubstr(R"("name":"int_col","type":"integer")"));
    EXPECT_THAT(schema_str, HasSubstr(R"("name":"str_col","type":"string")"));
    EXPECT_THAT(schema_str, HasSubstr(R"("name":"double_col","type":"double")"));
    EXPECT_THAT(schema_str, HasSubstr(R"("name":"date_col","type":"date")"));
    EXPECT_THAT(schema_str, HasSubstr(R"("name":"bool_col","type":"boolean")"));
}

TEST_F(ColumnTest, ComplexTypeDetection)
{
    // ---------------------------
    // Test Arrays and Maps
    // ---------------------------
    auto df = spark->sql("SELECT array(1, 2, 3) AS int_array, map('key', 1) AS string_int_map");

    auto schema = df.schema();

    ASSERT_EQ(schema.fields.size(), 2);

    // ---------------------------
    // Check Array
    // ---------------------------
    EXPECT_EQ(schema.fields[0].name, "int_array");
    EXPECT_TRUE(std::holds_alternative<spark::sql::types::ArrayType>(schema.fields[0].data_type.kind));

    // ---------------------------
    // Check Map
    // ---------------------------
    EXPECT_EQ(schema.fields[1].name, "string_int_map");
    EXPECT_TRUE(std::holds_alternative<spark::sql::types::MapType>(schema.fields[1].data_type.kind));
}