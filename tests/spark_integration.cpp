#include "spark_integration.h"
#include "env_loader.h"

SparkSession* SparkIntegrationTest::spark = nullptr;

void SparkIntegrationTest::SetUpTestSuite()
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

void SparkIntegrationTest::TearDownTestSuite()
{
    if (spark)
    {
        spark->stop();
    }
}
