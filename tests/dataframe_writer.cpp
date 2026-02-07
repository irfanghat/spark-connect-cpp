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
        .option("compression", "gzip")
        .parquet("output/range_data.parquet");

    auto read_data = spark->read().parquet("output/range_data.parquet");
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
        .parquet("output/iot_intrusion.parquet");

    auto iot_data_columns = spark->read()
                                .parquet("output/iot_intrusion.parquet")
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
        .parquet("output/iot_analysis_results.parquet");

    auto result_df = spark->read().parquet("output/iot_analysis_results.parquet");

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