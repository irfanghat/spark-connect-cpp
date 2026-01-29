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
// The following suite tests reading from various file formats i.e.
// json, csv, parquet, orc, avro and text files.
// ----------------------------------------------------------------

TEST_F(SparkIntegrationTest, ReadingJson)
{
    auto df = spark->read().json("datasets/people.json");
    // ------------------------------------------------------------------------------------
    // Using the fully qualified class name (JSON is usually built-in)
    // auto df = spark->read().format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").load({"datasets/people.json"});
    // ------------------------------------------------------------------------------------
    df.show();
}

TEST_F(SparkIntegrationTest, ReadJsonWithOptions)
{
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .json("datasets/people.json");

    EXPECT_NO_THROW(df.show(5));
}

TEST_F(SparkIntegrationTest, ReadingCsv)
{
    auto df = spark->read().csv("datasets/people.csv");
    EXPECT_NO_THROW(df.show(163));
}

TEST_F(SparkIntegrationTest, ReadCsvWithOptions)
{
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv("datasets/people.csv");

    EXPECT_NO_THROW(df.show());
}

TEST_F(SparkIntegrationTest, ReadTextFile)
{
    auto df = spark->read().text("datasets/people.txt");
    EXPECT_NO_THROW(df.show());
}

TEST_F(SparkIntegrationTest, ReadTextFileWithOptions)
{
    auto df = spark->read()
                  .option("wholetext", "true")
                  .option("lineSep", "\n")
                  .text("datasets/people.txt");

    EXPECT_NO_THROW(df.show());
}

TEST_F(SparkIntegrationTest, ReadParquet)
{
    auto df = spark->read().parquet("datasets/flights.parquet");
    EXPECT_NO_THROW(df.show(10000));
}

TEST_F(SparkIntegrationTest, ReadParquetWithOptions)
{
    // ----------------------------------------------------------------------------
    // Some Common Parquet options:
    //
    // "mergeSchema" - Merges schemas across all Parquet part-files which results in expensive operations
    // "datetimeRebaseMode" - Handles legacy dates from older Spark versions
    // ----------------------------------------------------------------------------
    auto df = spark->read()
                  .option("mergeSchema", "true")
                  .parquet("datasets/flights.parquet");

    EXPECT_NO_THROW(df.show(10));
}

TEST_F(SparkIntegrationTest, ReadWithDdlSchema)
{
    auto df = spark->read()
                  .schema("name STRING, age INT, salary INT")
                  .csv("datasets/people_no_header.csv");

    EXPECT_NO_THROW(df.show(4));
}

TEST_F(SparkIntegrationTest, SchemaEvaluation)
{
    std::string ddl = "name STRING, age INT, salary INT";
    auto df = spark->read().schema(ddl).csv("datasets/people_no_header.csv");

    auto schema = df.schema();

    ASSERT_EQ(schema.fields.size(), 3);
    EXPECT_EQ(schema.fields[0].name, "name");
    EXPECT_EQ(schema.fields[1].name, "age");

    // -------------------------------------------------------
    // Check if the variant holds the correct type
    // -------------------------------------------------------
    EXPECT_TRUE(std::holds_alternative<spark::sql::types::StringType>(schema.fields[0].data_type.kind));
    EXPECT_TRUE(std::holds_alternative<spark::sql::types::IntegerType>(schema.fields[1].data_type.kind));
}

TEST_F(SparkIntegrationTest, HandleMissingFile)
{
    auto df = spark->read().csv("datasets/non_existent.csv");

    // -----------------------------------------------------------------------------
    // Show triggers an ExecutePlan gRPC call which should fail in this scenario
    // -----------------------------------------------------------------------------
    EXPECT_THROW(df.show(), std::runtime_error);
}

TEST_F(SparkIntegrationTest, OptionEvaluation)
{
    // -------------------------------------------------------------------------
    // Read without header option (should have column name "value" or "_c0")
    // -------------------------------------------------------------------------
    auto df1 = spark->read().csv("datasets/people.csv");
    df1.show(2);

    // -----------------------------------
    // Read with header option
    // -----------------------------------
    auto df2 = spark->read().option("header", "true").csv("datasets/people.csv");
    df2.show(2);

    // --------------------------------------
    // The schemas should be different
    // --------------------------------------
    EXPECT_NE(df1.schema().fields[0].name, df2.schema().fields[0].name);
}