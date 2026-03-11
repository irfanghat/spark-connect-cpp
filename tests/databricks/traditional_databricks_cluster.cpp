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
#include "../util/env_loader.h"


class DatabaricksIntegrationTest : public ::testing::Test
{
protected:
    static SparkSession *spark;

    static void SetUpTestSuite()
    {
        load_env("../.env");

        const char *workspace_url = std::getenv("DATABRICKS_WORKSPACE_URL");
        const char *token = std::getenv("DATABRICKS_TOKEN");
        const char *cluster_id = std::getenv("DATABRICKS_CLUSTER_ID");

        spark = &SparkSession::builder()
                     .master(workspace_url)
                     .databricks(token, cluster_id)
                     .appName("spark-connect-cpp")
                     .getOrCreate();
    }
};

SparkSession *DatabaricksIntegrationTest::spark = nullptr;

TEST_F(DatabaricksIntegrationTest, DatabricksNycTaxiAnalysis)
{
    // ------------------------------------------------
    // This example validates Unity Catalog access
    // ------------------------------------------------
    EXPECT_NO_THROW(spark->sql("SELECT current_user(), current_catalog()").show());
}