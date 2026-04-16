#pragma once

#include "session.h"
#include <gtest/gtest.h>

class SparkIntegrationTest : public ::testing::Test
{
  protected:
    static SparkSession* spark;

    static void SetUpTestSuite();
    static void TearDownTestSuite();
};
