#pragma once

#include "session.h"
#include <gtest/gtest.h>

class DatabricksServerlessIntegrationTest : public ::testing::Test
{
  protected:
    static SparkSession* spark;

    static void SetUpTestSuite();
};
