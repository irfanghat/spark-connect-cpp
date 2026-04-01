#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gmock/gmock-matchers.h>

#include "runtime_config.h"

#include <sstream>

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
        // ------------------------------------------------------------------
        // Exercise builder.config() at session creation time
        // This is the primary use-case:
        //
        // Configs set before the session exists are flushed
        // immediately after the gRPC channel is established.
        // ------------------------------------------------------------------
        spark = &SparkSession::builder()
                     .master("sc://localhost")
                     .appName("SparkConnectCppGTest")
                     .config("spark.sql.shuffle.partitions", int64_t(42))
                     .config("spark.sql.ansi.enabled", true)
                     .config("spark.sql.session.timeZone", std::string("America/New_York"))
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
            spark->conf().unset("spark.app.name");
        }
        catch (...)
        {
        }
    }
};

SparkSession *SparkConfigurationIntegrationTest::spark = nullptr;

// ----------------------------------------------------------------------------------
// builder.config() - configs set on the builder are flushed to the live session.
//
// The three assertions verify configs applied at session creation time in SetUpTestSuite.
// Testing them this way avoids key resets between assertions (Calling TearDown() after each fixture).
// ----------------------------------------------------------------------------------
TEST_F(SparkConfigurationIntegrationTest, BuilderConfigAppliedAtCreation)
{
    EXPECT_EQ(spark->conf().get("spark.sql.shuffle.partitions"), "42");
    EXPECT_EQ(spark->conf().get("spark.sql.ansi.enabled"), "true");
    EXPECT_EQ(spark->conf().get("spark.sql.session.timeZone"), "America/New_York");
}

TEST_F(SparkConfigurationIntegrationTest, BuilderConfigFlushesOnSubsequentGetOrCreate)
{
    // ------------------------------------------------------------------
    // builder.config() on an already-running singleton still flushes —
    // getOrCreate() applies runtime_configs regardless of whether the
    // session was just created or already existed.
    // ------------------------------------------------------------------
    SparkSession::builder()
        .config("spark.sql.shuffle.partitions", int64_t(99))
        .getOrCreate();

    EXPECT_EQ(spark->conf().get("spark.sql.shuffle.partitions"), "99");
}

TEST_F(SparkConfigurationIntegrationTest, BuilderConfigRawIntOverloading)
{
    SparkSession::builder()
        .config("spark.sql.shuffle.partitions", 99)
        .getOrCreate();

    EXPECT_EQ(spark->conf().get("spark.sql.shuffle.partitions"), "99");
}

TEST_F(SparkConfigurationIntegrationTest, BuilderConfigChainingMultipleKeys)
{
    SparkSession::builder()
        .config("spark.sql.shuffle.partitions", int64_t(8))
        .config("spark.sql.ansi.enabled", false)
        .config("spark.sql.session.timeZone", std::string("UTC"))
        .getOrCreate();

    EXPECT_EQ(spark->conf().get("spark.sql.shuffle.partitions"), "8");
    EXPECT_EQ(spark->conf().get("spark.sql.ansi.enabled"), "false");
    EXPECT_EQ(spark->conf().get("spark.sql.session.timeZone"), "UTC");
}

TEST_F(SparkConfigurationIntegrationTest, BuilderConfigMapOverload)
{
    SparkSession::builder()
        .config(std::map<std::string, std::string>{
            {"spark.sql.shuffle.partitions", "12"},
            {"spark.sql.session.timeZone", "America/Chicago"},
        })
        .getOrCreate();

    EXPECT_EQ(spark->conf().get("spark.sql.shuffle.partitions"), "12");
    EXPECT_EQ(spark->conf().get("spark.sql.session.timeZone"), "America/Chicago");
}

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
    // spark.sql.shuffle.partitions default is 200
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
//
// spark.checkpoint.dir is NOT modifiable via the Spark Connect Config RPC.
// The server rejects changes with CANNOT_MODIFY_CONFIG. It must be set
// at cluster creation time in the cluster/session configuration.
// ----------------------------------------------------------------------------------
TEST_F(SparkConfigurationIntegrationTest, SetCheckpointDirIsNotModifiable)
{
    EXPECT_FALSE(spark->conf().isModifiable("spark.checkpoint.dir"));
}

TEST_F(SparkConfigurationIntegrationTest, SetCheckpointDirThrows)
{
    EXPECT_THROW(
        spark->setCheckpointDir("/tmp/cpp-test-checkpoints"),
        std::runtime_error);
}

// ----------------------------------------------------------------------------------
// get() - error handling
// ----------------------------------------------------------------------------------
TEST_F(SparkConfigurationIntegrationTest, GetUnknownKeyThrows)
{
    EXPECT_THROW(
        spark->conf().get("spark.cpp.test.nonexistent.key"),
        std::runtime_error);
}
