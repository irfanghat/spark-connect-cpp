#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gmock/gmock-matchers.h>

#include "session.h"
#include "config.h"
#include "dataframe.h"
#include "../util/env_loader.h"

#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdlib>

class DatabricksServerlessIntegrationTest : public ::testing::Test
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

SparkSession *DatabricksServerlessIntegrationTest::spark = nullptr;

TEST_F(DatabricksServerlessIntegrationTest, DatabricksNycTaxiAnalysis_Serverless)
{
    // ------------------------------------------------
    // Querying the public Databricks samples dataset
    //
    // This example performs analysis on the trip distance
    // and fare amounts using the 'nyc trips' sample dataset
    // ------------------------------------------------
    auto df = spark->sql(R"(
        SELECT 
            pickup_zip, 
            COUNT(*) AS total_trips, 
            ROUND(AVG(fare_amount), 2) AS avg_fare, 
            MAX(trip_distance) AS longest_trip 
        FROM samples.nyctaxi.trips 
        WHERE fare_amount > 0 
        GROUP BY pickup_zip 
        ORDER BY total_trips DESC
    )");

    df.show(20);

    ASSERT_GT(df.count(), 0) << "The taxi dataset should not be empty.";
}