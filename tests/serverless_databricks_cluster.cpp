#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gmock/gmock-matchers.h>

#include "session.h"
#include "config.h"
#include "dataframe.h"
#include "env_loader.h"
#include "spark_integration.h"

#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdlib>


TEST_F(SparkIntegrationTest, DatabricksNycTaxiAnalysis_Serverless)
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