#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gmock/gmock-matchers.h>

#include "session.h"
#include "config.h"
#include "dataframe.h"
#include "spark_integration.h"
#include "env_loader.h"

#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdlib>


TEST_F(SparkIntegrationTest, DatabricksNycTaxiAnalysis)
{
    // ------------------------------------------------
    // This example validates Unity Catalog access
    // ------------------------------------------------
    EXPECT_NO_THROW(spark->sql("SELECT current_user(), current_catalog()").show());
}