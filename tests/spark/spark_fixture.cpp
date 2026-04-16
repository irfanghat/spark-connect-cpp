#include "spark_fixture.h"
#include "../util/env_loader.h"

SparkSession* SparkIntegrationTest::spark = nullptr;

void SparkIntegrationTest::SetUpTestSuite()
{
    spark =
        &SparkSession::builder().master("localhost").appName("SparkConnectCppGTest").getOrCreate();
}

void SparkIntegrationTest::TearDownTestSuite()
{
    if (spark)
    {
        spark->stop();
    }
}
