#pragma once

#include <gtest/gtest.h>
#include "session.h"

class DatabricksServerlessIntegrationTest : public ::testing::Test
{
    protected:

        static SparkSession *spark;

        static void SetUpTestSuite();
};
