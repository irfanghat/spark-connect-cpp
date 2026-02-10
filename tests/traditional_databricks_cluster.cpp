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
        const char *cluster_id = std::getenv("DATABRICKS_CLUSTER_ID");

        spark = &SparkSession::builder()
                     .master(workspace_url)
                     .databricks(token, cluster_id)
                     .appName("spark-connect-cpp")
                     .getOrCreate();
    }
};

SparkSession *SparkIntegrationTest::spark = nullptr;

TEST_F(SparkIntegrationTest, DatabricksNycTaxiAnalysis)
{
    // ------------------------------------------------
    // This example validates Unity Catalog access
    // ------------------------------------------------
    EXPECT_NO_THROW(spark->sql("SELECT current_user(), current_catalog()").show());
}