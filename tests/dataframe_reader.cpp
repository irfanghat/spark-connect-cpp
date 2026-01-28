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

TEST_F(SparkIntegrationTest, ReadingCsv)
{
    auto df = spark->read().csv("datasets/people.csv");
    df.show(163);
}

TEST_F(SparkIntegrationTest, ReadCsvWithOptions)
{
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv("datasets/people.csv");

    df.show();
}