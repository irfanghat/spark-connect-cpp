#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <iostream>
#include <map>

#include "session.h"
#include "config.h"
#include "dataframe.h"
#include "writer.h"

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

TEST_F(SparkIntegrationTest, ParquetWrite)
{
    auto write_data = spark->range(100);

    write_data.filter("id > 50")
        .write()
        .mode("overwrite")
        .option("compression", "gzip")
        .parquet("output/range_data.parquet");

    auto read_data = spark->read().parquet("output/range_data.parquet");
    read_data.show(100);
}