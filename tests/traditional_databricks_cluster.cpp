#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gmock/gmock-matchers.h>

#include "session.h"
#include "config.h"
#include "dataframe.h"

#include <iostream>
#include <cstdlib>
#include <algorithm>
#include <fstream>
#include <sstream>

static inline std::string trim(const std::string& s)
{
    auto start = s.find_first_not_of(" \t\r\n");
    auto end = s.find_last_not_of(" \t\r\n");
    if (start == std::string::npos)
        return "";
    return s.substr(start, end - start + 1);
}

void load_env(const std::string& path)
{
    std::ifstream file(path);

    if (!file.is_open())
    {
        throw std::runtime_error("Failed to open .env file: " + path);
    }

    std::string line;

    while (std::getline(file, line))
    {
        line = trim(line);

        // skip empty lines and comments
        if (line.empty() || line[0] == '#')
            continue;

        auto pos = line.find('=');
        if (pos == std::string::npos)
            continue;

        std::string key = trim(line.substr(0, pos));
        std::string val = trim(line.substr(pos + 1));

        // remove optional quotes
        if (!val.empty() && val.front() == '"' && val.back() == '"')
        {
            val = val.substr(1, val.size() - 2);
        }

        setenv(key.c_str(), val.c_str(), 1);
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