#pragma once

#include <gtest/gtest.h>
#include "session.h"

class SparkIntegrationTest : public ::testing::Test
{
    protected:
    
        static SparkSession* spark;

        static void SetUpTestSuite();
        static void TearDownTestSuite();
};
