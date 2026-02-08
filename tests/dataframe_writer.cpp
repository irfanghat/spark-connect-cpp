#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gmock/gmock-matchers.h>

#include <iostream>
#include <map>

#include "session.h"
#include "config.h"
#include "dataframe.h"
#include "writer.h"

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

TEST_F(SparkIntegrationTest, ParquetWrite)
{
    auto write_data = spark->range(100);

    write_data.filter("id > 50")
        .write()
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet("output/parquet_range_data");

    auto read_data = spark->read().parquet("output/parquet_range_data");
    read_data.show(100);

    EXPECT_EQ(read_data.count(), 49);
}

TEST_F(SparkIntegrationTest, ParquetReadAndWrite)
{
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv("datasets/iot_intrusion_data.csv");

    df.printSchema();

    df.write()
        .mode("overwrite")
        .option("compression", "snappy") // snappy is faster when compared to gzip. See: https://parquet.apache.org/docs/file-format/data-pages/compression/
        .parquet("output/iot_intrusion_data");

    auto iot_data_columns = spark->read()
                                .parquet("output/iot_intrusion_data")
                                .columns();

    EXPECT_THAT(iot_data_columns, ElementsAre("flow_duration", "Header_Length", "Protocol Type", "Duration", "Rate", "Srate", "Drate", "fin_flag_number", "syn_flag_number", "rst_flag_number", "psh_flag_number", "ack_flag_number", "ece_flag_number", "cwr_flag_number", "ack_count", "syn_count", "fin_count", "urg_count", "rst_count", "HTTP", "HTTPS", "DNS", "Telnet", "SMTP", "SSH", "IRC", "TCP", "UDP", "DHCP", "ARP", "ICMP", "IPv", "LLC", "Tot sum", "Min", "Max", "AVG", "Std", "Tot size", "IAT", "Number", "Magnitue", "Radius", "Covariance", "Variance", "Weight", "label"));
}

TEST_F(SparkIntegrationTest, IOTSecurityAnalysisScenario)
{
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv("datasets/iot_intrusion_data.csv");

    // -----------------------------------------------------------------------------------
    // Filter for specific DDoS indicators (e.g., Rate > 0.5 and specific Protocol Type)
    // and project only the columns needed for a security report.
    // -----------------------------------------------------------------------------------
    auto analysis_df = df.filter("Rate > 0.2")
                           .select({"label", "Rate", "Protocol Type", "flow_duration", "IAT"});

    analysis_df.write()
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet("output/iot_analysis_results");

    auto result_df = spark->read().parquet("output/iot_analysis_results");

    // --------------------------------------------------------
    // Validate that filter and projection were preserved
    // --------------------------------------------------------
    auto result_cols = result_df.columns();
    EXPECT_THAT(result_cols, ElementsAre("label", "Rate", "Protocol Type", "flow_duration", "IAT"));

    // ---------------------------------------------------------------
    // Get count of the detected incidents in filtered result
    // ---------------------------------------------------------------
    int64_t incident_count = result_df.count();
    std::cout << "Detected " << incident_count << " high-rate traffic incidents." << std::endl;

    result_df.show(5);

    EXPECT_GT(incident_count, 0);
}

TEST_F(SparkIntegrationTest, CsvWriteWithOptions)
{
    auto write_df = spark->range(50);
    std::string csv_path = "output/csv_write_with_options";

    write_df.write()
        .mode("overwrite")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(csv_path);

    auto read_df = spark->read()
                       .option("header", "true")
                       .option("delimiter", ";")
                       .csv(csv_path);

    EXPECT_EQ(read_df.count(), 50);
    EXPECT_THAT(read_df.columns(), ElementsAre("id"));
}

TEST_F(SparkIntegrationTest, WriteToText)
{
    auto write_df = spark->sql("SELECT 'a' AS alphabets UNION ALL SELECT 'b' AS alphabets");

    write_df.write()
        .mode("overwrite")
        .text("output/text_write");

    auto read_df = spark->read().text("output/text_write");

    EXPECT_THAT(read_df.columns(), ElementsAre("value"));
    EXPECT_EQ(read_df.count(), 2);
}

TEST_F(SparkIntegrationTest, EmptyDataFrameWrite)
{
    auto empty_df = spark->range(0);

    empty_df.write()
        .mode("overwrite")
        .parquet("output/empty_data_parquet");

    auto read_back = spark->read().parquet("output/empty_data_parquet");

    EXPECT_EQ(read_back.count(), 0);
    EXPECT_THAT(read_back.columns(), ElementsAre("id"));
}

TEST_F(SparkIntegrationTest, MultiColumnPartitioning)
{
    // -----------------------------------------------------------
    // Partitioning by multiple columns
    // This mostly focuses on the WriteOperation proto mapping
    // -----------------------------------------------------------
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv("datasets/iot_intrusion_data.csv");

    // ------------------------------------------------------
    // Take a subset and partition by Protocol and Label
    // ------------------------------------------------------
    df.limit(100).write().mode("overwrite").partitionBy({"Protocol Type", "label"}).parquet("output/multi_partitioned_iot");

    auto read_df = spark->read().parquet("output/multi_partitioned_iot");

    // ----------------------------------------------------------------------
    // Verify columns exist (Partition columns move to the end in Spark)
    // ----------------------------------------------------------------------
    auto cols = read_df.columns();
    bool has_protocol = std::find(cols.begin(), cols.end(), "Protocol Type") != cols.end();
    bool has_label = std::find(cols.begin(), cols.end(), "label") != cols.end();

    EXPECT_TRUE(has_protocol);
    EXPECT_TRUE(has_label);
}

TEST_F(SparkIntegrationTest, SaveModeIgnoreBehavior)
{
    // ---------------------------------
    // Evaluate "ignore" save mode
    // ---------------------------------
    auto path = "output/ignore_test_parquet";
    auto df1 = spark->range(10);
    auto df2 = spark->range(100);

    df1.write().mode("overwrite").parquet(path);

    // -----------------------------------------------------------
    // Second write with 'ignore' should not change the data
    // -----------------------------------------------------------
    df2.write().mode("ignore").parquet(path);

    auto result = spark->read().parquet(path);
    EXPECT_EQ(result.count(), 10); // Row count should still be 10, not 100
}

TEST_F(SparkIntegrationTest, JsonReadWrite)
{
    auto df = spark->sql("SELECT 1 as id, 'test' as name, NAMED_STRUCT('a', 1, 'b', 2) as nested");

    df.write()
        .mode("overwrite")
        .format("json")
        .save("output/test_json");

    auto read_df = spark->read().format("json").load({"output/test_json"});

    EXPECT_EQ(read_df.count(), 1);
    EXPECT_THAT(read_df.columns(), ::testing::UnorderedElementsAre("id", "name", "nested"));
}

TEST_F(SparkIntegrationTest, PartitionedWriteToParquet)
{
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv("datasets/iot_intrusion_data.csv");

    // ----------------------------------------
    // Partition by the 'label' column
    // ----------------------------------------
    df.limit(100).write().mode("overwrite").partitionBy({"label"}).parquet("output/partitioned_iot");

    // ------------------------------------------------------------------------------------------
    // Spark creates directory structures such as:
    //
    // output/partitioned_iot/label=DDoS-RSTFINFlood/
    // output/partitioned_iot/label=DDoS-SynonymousIP_Flood
    // etc...
    // ------------------------------------------------------------------------------------------
    auto read_partitioned = spark->read().parquet("output/partitioned_iot");
    EXPECT_GT(read_partitioned.count(), 0);
}