#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gmock/gmock-matchers.h>

#include "session.h"
#include "dataframe.h"

using ::testing::Contains;
using ::testing::Pair;

class SparkConfigurationIntegrationTest : public ::testing::Test
{
protected:
    static SparkSession *spark;

    static void SetUpTestSuite()
    {
        spark = &SparkSession::builder()
                     .master("sc://localhost")
                     .appName("SparkConnectCppGTest")
                     .getOrCreate();
    }

    static void TearDownTestSuite()
    {
        if (spark)
            spark->stop();
    }

    // -----------------------------------------------------------------
    // Restore keys touched by tests so each test starts clean
    // -----------------------------------------------------------------
    void TearDown() override
    {
        try
        {
            spark->conf().unset("spark.sql.session.timeZone");
        }
        catch (...)
        {
        }
        try
        {
            spark->conf().unset("spark.sql.shuffle.partitions");
        }
        catch (...)
        {
        }
        try
        {
            spark->conf().unset("spark.sql.ansi.enabled");
        }
        catch (...)
        {
        }
        try
        {
            spark->conf().unset("spark.checkpoint.dir");
        }
        catch (...)
        {
        }
        try
        {
            spark->conf().unset("spark.app.name");
        }
        catch (...)
        {
        }
    }
};

SparkSession *SparkConfigurationIntegrationTest::spark = nullptr;

// ----------------------------------------------------------------------------------
// set() / get() - round-trip
//
// Spark 3.5 validates config values against their declared types server-side.
// @example
//   string  ->  spark.sql.session.timeZone   (STRING, modifiable, default "UTC")
//   int     ->  spark.sql.shuffle.partitions  (INT type)
//   bool    ->  spark.sql.ansi.enabled        (BOOLEAN type)
//
// @note
// spark.app.name is NOT modifiable at runtime - set() is silently
// ignored and get() returns the value baked in at session creation.
// ----------------------------------------------------------------------------------
TEST_F(SparkConfigurationIntegrationTest, SetAndGetString)
{
    spark->conf().set("spark.sql.session.timeZone", "America/New_York");
    EXPECT_EQ(spark->conf().get("spark.sql.session.timeZone"), "America/New_York");
}

TEST_F(SparkConfigurationIntegrationTest, SetBoolTrue)
{
    spark->conf().set("spark.sql.ansi.enabled", true);
    EXPECT_EQ(spark->conf().get("spark.sql.ansi.enabled"), "true");
}

TEST_F(SparkConfigurationIntegrationTest, SetBoolFalse)
{
    spark->conf().set("spark.sql.ansi.enabled", false);
    EXPECT_EQ(spark->conf().get("spark.sql.ansi.enabled"), "false");
}

TEST_F(SparkConfigurationIntegrationTest, SetInt64)
{
    spark->conf().set("spark.sql.shuffle.partitions", int64_t(16));
    EXPECT_EQ(spark->conf().get("spark.sql.shuffle.partitions"), "16");
}

TEST_F(SparkConfigurationIntegrationTest, OverwriteExistingKey)
{
    spark->conf().set("spark.sql.shuffle.partitions", int64_t(10));
    spark->conf().set("spark.sql.shuffle.partitions", int64_t(20));
    EXPECT_EQ(spark->conf().get("spark.sql.shuffle.partitions"), "20");
}

// ----------------------------------------------------------------------------------
// get() with default
// ----------------------------------------------------------------------------------
TEST_F(SparkConfigurationIntegrationTest, GetWithDefaultReturnsValueWhenSet)
{
    spark->conf().set("spark.sql.shuffle.partitions", int64_t(8));
    EXPECT_EQ(spark->conf().get("spark.sql.shuffle.partitions", "200"), "8");
}

TEST_F(SparkConfigurationIntegrationTest, GetWithDefaultReturnsDefaultWhenUnset)
{
    const std::string key = "spark.cpp.test.nonexistent.key";
    EXPECT_EQ(spark->conf().get(key, "fallback"), "fallback");
}

// ----------------------------------------------------------------------------------
// getOption()
// ----------------------------------------------------------------------------------
TEST_F(SparkConfigurationIntegrationTest, GetOptionReturnsSomeWhenSet)
{
    spark->conf().set("spark.sql.shuffle.partitions", int64_t(7));
    auto val = spark->conf().getOption("spark.sql.shuffle.partitions");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "7");
}

TEST_F(SparkConfigurationIntegrationTest, GetOptionReturnsNulloptWhenUnset)
{
    auto val = spark->conf().getOption("spark.cpp.test.nonexistent.key");
    EXPECT_FALSE(val.has_value());
}

// ----------------------------------------------------------------------------------
// unset()
// ----------------------------------------------------------------------------------
TEST_F(SparkConfigurationIntegrationTest, UnsetRevertsToDefault)
{
    // -----------------------------------------------------------------
    // spark.sql.shuffle.partitions is set to 200 by default
    // -----------------------------------------------------------------
    spark->conf().set("spark.sql.shuffle.partitions", int64_t(42));
    spark->conf().unset("spark.sql.shuffle.partitions");
    EXPECT_EQ(spark->conf().get("spark.sql.shuffle.partitions", "200"), "200");
}

TEST_F(SparkConfigurationIntegrationTest, UnsetMakesGetOptionReturnNullopt)
{
    spark->conf().set("spark.app.name", "to-be-removed");
    spark->conf().unset("spark.app.name");
    auto val = spark->conf().getOption("spark.cpp.test.nonexistent.key");
    EXPECT_FALSE(val.has_value());
}

// ----------------------------------------------------------------------------------
// getAll()
// ----------------------------------------------------------------------------------
TEST_F(SparkConfigurationIntegrationTest, GetAllReturnsNonEmptyMap)
{
    auto all = spark->conf().getAll();
    EXPECT_FALSE(all.empty());
}

TEST_F(SparkConfigurationIntegrationTest, GetAllContainsSetKey)
{
    spark->conf().set("spark.sql.shuffle.partitions", int64_t(33));
    auto all = spark->conf().getAll();
    EXPECT_THAT(all, Contains(Pair("spark.sql.shuffle.partitions", "33")));
}

// ----------------------------------------------------------------------------------
// isModifiable()
// ----------------------------------------------------------------------------------
TEST_F(SparkConfigurationIntegrationTest, ShufflePartitionsIsModifiable)
{
    EXPECT_TRUE(spark->conf().isModifiable("spark.sql.shuffle.partitions"));
}

TEST_F(SparkConfigurationIntegrationTest, SparkMasterIsNotModifiable)
{
    // -------------------------------------------------------------------------
    // spark.master is locked at session creation and cannot be changed
    // -------------------------------------------------------------------------
    EXPECT_FALSE(spark->conf().isModifiable("spark.master"));
}

// ----------------------------------------------------------------------------------
// setCheckpointDir()
// ----------------------------------------------------------------------------------
TEST_F(SparkConfigurationIntegrationTest, SetCheckpointDirIsReadBack)
{
    spark->setCheckpointDir("/tmp/cpp-test-checkpoints");
    auto val = spark->conf().getOption("spark.checkpoint.dir");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "/tmp/cpp-test-checkpoints");
}

TEST_F(SparkConfigurationIntegrationTest, GetUnknownKeyThrows)
{
    EXPECT_THROW(
        spark->conf().get("spark.cpp.test.nonexistent.key"),
        std::runtime_error);
}