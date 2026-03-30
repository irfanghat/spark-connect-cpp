#include "databricks_serverless_fixture.h"
#include "../util/env_loader.h"


SparkSession *DatabricksServerlessIntegrationTest::spark = nullptr;

void DatabricksServerlessIntegrationTest::SetUpTestSuite()
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
