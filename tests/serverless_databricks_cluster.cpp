#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gmock/gmock-matchers.h>

#include "session.h"
#include "config.h"
#include "dataframe.h"

#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdlib>

void load_env(const std::string &path)
{
    std::ifstream file(path);
    std::string line;
    while (std::getline(file, line))
    {
        auto pos = line.find('=');
        if (pos != std::string::npos)
        {
            auto key = line.substr(0, pos);
            auto val = line.substr(pos + 1);
            setenv(key.c_str(), val.c_str(), 1);
        }
    }
}

class SparkIntegrationTest : public ::testing::Test
{
protected:
    static SparkSession *spark;

    static void SetUpTestSuite()
    {
        load_env("../.env");

        const char *workspace_url = std::getenv("DATABRICKS_WORKSPACE_URL");
        const char *token = std::getenv("DATABRICKS_TOKEN");
        const char *warehouse_id = std::getenv("DATABRICKS_WAREHOUSE_ID");

        spark = &SparkSession::builder()
                     .master(workspace_url)
                     .serverless(token, warehouse_id)
                     .appName("spark-connect-cpp")
                     .getOrCreate();
    }
};

SparkSession *SparkIntegrationTest::spark = nullptr;

TEST_F(SparkIntegrationTest, DatabricksNycTaxiAnalysis)
{
    // ------------------------------------------------
    // Querying the public Databricks samples dataset
    //
    // This example performs analysis on the trip distance
    // and fare amounts using the 'nyc trips' sample dataset
    // ------------------------------------------------
    auto df = spark->sql(
        "SELECT "
        "  pickup_zip, "
        "  COUNT(*) as total_trips, "
        "  ROUND(AVG(fare_amount), 2) as avg_fare, "
        "  MAX(trip_distance) as longest_trip "
        "FROM samples.nyctaxi.trips "
        "WHERE fare_amount > 0 "
        "GROUP BY pickup_zip "
        "ORDER BY total_trips DESC");

    df.show(20);

    ASSERT_GT(df.count(), 0) << "The taxi dataset should not be empty.";
}