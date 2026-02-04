#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <iostream>
#include <map>

#include "session.h"
#include "config.h"
#include "dataframe.h"

class SparkIntegrationTest : public ::testing::Test
{
protected:
    static SparkSession *spark;

    static void SetUpTestSuite()
    {
        spark = &SparkSession::builder()
                     .master("localhost")
                     .appName("SparkConnectCppGTest")
                     .getOrCreate();
    }

    static void TearDownTestSuite()
    {
        if (spark)
        {
            spark->stop();
        }
    }
};

SparkSession *SparkIntegrationTest::spark = nullptr;

// ----------------------------------------------------------------
// The following suite tests reading from various file formats i.e.
// json, csv, parquet, orc, avro and text files.
// ----------------------------------------------------------------

TEST_F(SparkIntegrationTest, ReadingJson)
{
    auto df = spark->read().json("datasets/people.json");
    // ------------------------------------------------------------------------------------
    // Using the fully qualified class name (JSON is usually built-in)
    // auto df = spark->read().format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").load({"datasets/people.json"});
    // ------------------------------------------------------------------------------------
    df.show();
}

TEST_F(SparkIntegrationTest, ReadJsonWithOptions)
{
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .json("datasets/people.json");

    EXPECT_NO_THROW(df.show(5));
}

TEST_F(SparkIntegrationTest, ReadingCsv)
{
    auto df = spark->read().csv("datasets/people.csv");
    EXPECT_NO_THROW(df.show(163));
}

TEST_F(SparkIntegrationTest, ReadCsvWithOptions)
{
    auto df = spark->read()
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv("datasets/people.csv");

    EXPECT_NO_THROW(df.show());
}

TEST_F(SparkIntegrationTest, ReadTextFile)
{
    auto df = spark->read().text("datasets/people.txt");
    EXPECT_NO_THROW(df.show());
}

TEST_F(SparkIntegrationTest, ReadTextFileWithOptions)
{
    auto df = spark->read()
                  .option("wholetext", "true")
                  .option("lineSep", "\n")
                  .text("datasets/people.txt");

    EXPECT_NO_THROW(df.show());
}

TEST_F(SparkIntegrationTest, ReadParquet)
{
    auto df = spark->read().parquet("datasets/flights.parquet");
    EXPECT_NO_THROW(df.show(1000));
}

TEST_F(SparkIntegrationTest, ReadParquetWithOptions)
{
    // ----------------------------------------------------------------------------
    // Some Common Parquet options:
    //
    // "mergeSchema" - Merges schemas across all Parquet part-files which results in expensive operations
    // "datetimeRebaseMode" - Handles legacy dates from older Spark versions
    // ----------------------------------------------------------------------------
    auto df = spark->read()
                  .option("mergeSchema", "true")
                  .parquet("datasets/flights.parquet");

    EXPECT_NO_THROW(df.show(10));
}

TEST_F(SparkIntegrationTest, ReadWithDdlSchema)
{
    auto df = spark->read()
                  .schema("name STRING, age INT, salary INT")
                  .csv("datasets/people_no_header.csv");

    EXPECT_NO_THROW(df.show(4));
}

TEST_F(SparkIntegrationTest, SchemaEvaluation)
{
    std::string ddl = "name STRING, age INT, salary INT";
    auto df = spark->read().schema(ddl).csv("datasets/people_no_header.csv");

    auto schema = df.schema();

    ASSERT_EQ(schema.fields.size(), 3);
    EXPECT_EQ(schema.fields[0].name, "name");
    EXPECT_EQ(schema.fields[1].name, "age");

    // -------------------------------------------------------
    // Check if the variant holds the correct type
    // -------------------------------------------------------
    EXPECT_TRUE(std::holds_alternative<spark::sql::types::StringType>(schema.fields[0].data_type.kind));
    EXPECT_TRUE(std::holds_alternative<spark::sql::types::IntegerType>(schema.fields[1].data_type.kind));
}

TEST_F(SparkIntegrationTest, HandleMissingFile)
{
    auto df = spark->read().csv("datasets/non_existent.csv");

    // -----------------------------------------------------------------------------
    // Show triggers an ExecutePlan gRPC call which should fail in this scenario
    // -----------------------------------------------------------------------------
    EXPECT_THROW(df.show(), std::runtime_error);
}

TEST_F(SparkIntegrationTest, OptionEvaluation)
{
    // -------------------------------------------------------------------------
    // Read without header option (should have column name "value" or "_c0")
    // -------------------------------------------------------------------------
    auto df1 = spark->read().csv("datasets/people.csv");
    df1.show(2);

    // -----------------------------------
    // Read with header option
    // -----------------------------------
    auto df2 = spark->read().option("header", "true").csv("datasets/people.csv");
    df2.show(2);

    // --------------------------------------
    // The schemas should be different
    // --------------------------------------
    EXPECT_NE(df1.schema().fields[0].name, df2.schema().fields[0].name);
}

/**
 * @brief Configuration options for the Spark DataFrameReader.
 * * These options control how the underlying Spark engine parses source files (CSV, JSON, etc.)
 * into C++ Row objects. Use these keys within a std::map<std::string, std::string>
 * passed to the .options() method.
 *
 * Schema Discovery & Integrity
 * - @b "inferSchema": (Boolean) If "true", Spark performs a first pass over the data to
 * automatically determine column types (e.g., Integer, Double). If "false" (default),
 * all columns are treated as StringType.
 * - @b "header": (Boolean) If "true", the first line of the file is used for column names.
 * If "false", columns are named automatically (e.g., _c0, _c1).
 * - @b "enforceSchema": (Boolean) If "true", forces the data to match a user-defined
 * schema, returning null for non-compliant fields.
 *
 * CSV Parsing Configuration
 * - @b "delimiter": (String) The character separating fields. Default is ','.
 * - @b "quote": (String) The character used for escaping values containing the delimiter.
 * Default is '"'.
 * - @b "escape": (String) The character used for escaping quotes inside a value.
 * Default is '\'.
 * - @b "nullValue": (String) Defines which string represents a NULL in the dataset.
 *
 * Temporal (Date/Time) Formats
 * - @b "dateFormat": (String) Pattern used to parse DateType columns (e.g., "yyyy-MM-dd").
 * - @b "timestampFormat": (String) Pattern used for TimestampType columns.
 *
 * Corruption & Error Handling Modes
 * - @b "mode": Determines behavior when a malformed row is encountered:
 * - "PERMISSIVE": Sets corrupted fields to null (Default).
 * - "DROPMALFORMED": Discards the entire row.
 * - "FAILFAST": Throws an exception and terminates the job immediately.
 * * @note When using CSVs without a manual schema, @ref inferSchema must be "true"
 * for Row::get_long() or Row::get_double() to succeed, as it prevents numeric
 * data from being stored as std::string in the internal variant.
 */

TEST_F(SparkIntegrationTest, ReaderOptions_VerifySchema)
{
    std::map<std::string, std::string> config = {
        {"header", "true"},
        {"inferSchema", "true"},
        {"delimiter", ","}};

    auto df = spark->read().options(config).format("csv").load({"datasets/people.csv"});

    auto cols = df.columns();

    // ----------------------------------------------------------------
    // Check if column names were read from the header correctly
    // ----------------------------------------------------------------
    ASSERT_EQ(cols.size(), 3);
    EXPECT_EQ(cols[0], "name");
    EXPECT_EQ(cols[1], "age");
}

TEST_F(SparkIntegrationTest, ReaderOptions_InferredTypes)
{
    std::map<std::string, std::string> config = {
        {"header", "true"},
        {"inferSchema", "true"}};

    auto df = spark->read().options(config).format("csv").load({"datasets/people.csv"});
    auto schema = df.schema();

    // ----------------------------------------------------
    // Find the 'age' field and verify it's an Integer
    // ----------------------------------------------------
    auto it = std::find_if(schema.fields.begin(), schema.fields.end(),
                           [](const auto &f)
                           { return f.name == "age"; });

    ASSERT_NE(it, schema.fields.end()) << "Column 'age' not found";

    // -------------------------------------------------
    // Check if the variant holds IntegerType
    // -------------------------------------------------
    EXPECT_TRUE(std::holds_alternative<spark::sql::types::IntegerType>(it->data_type.kind));
}

TEST_F(SparkIntegrationTest, ReaderOptions_CorrectValueParsing)
{
    std::map<std::string, std::string> config = {{"header", "true"}, {"delimiter", ","}, {"inferSchema", "true"}};
    auto df = spark->read().options(config).format("csv").load({"datasets/people.csv"});

    auto first_row = df.head();
    ASSERT_TRUE(first_row.has_value());

    // --------------------------------------------------
    // Verify values via the templated get<T>
    // --------------------------------------------------
    EXPECT_EQ(first_row->get<std::string>("name"), "John");
    EXPECT_EQ(first_row->get_long("age"), 25);
}
