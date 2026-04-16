#include <gtest/gtest.h>
#include "session.h"
#include "ml/feature/tokenizer.h"
#include "ml/feature/regextokenizer.h"

TEST(TokenizerTest, BasicTransform) {
    auto spark = &SparkSession::builder()
                     .master("sc://localhost")
                     .appName("TokenizerTest")
                     .getOrCreate();

    auto df = spark->sql(
        "SELECT 1 AS id, 'Hello World, foo bar' AS text "
        "UNION ALL "
        "SELECT 2, 'Apache Spark is great' "
        "UNION ALL "
        "SELECT 3, 'regex tokenizer test'"
    );

    Tokenizer tok("text", "tokens");
    auto result = tok.transform(df);

    // verify output column exists
    result.show();
    SUCCEED();
}

TEST(RegexTokenizerTest, BasicTransform) {
    auto spark = &SparkSession::builder()
                     .master("sc://localhost")
                     .appName("RegexTokenizerTest")
                     .getOrCreate();

    auto df = spark->sql(
        "SELECT 1 AS id, 'Hello Wor!ld$ foo_bar' AS text "
        "UNION ALL "
        "SELECT 2, 'Apache Spark is great' "
        "UNION ALL "
        "SELECT 3, 'regex tokenizer test'"
    );

    RegexTokenizer regex_tok("text", "tokens", "\\W+");
    auto result = regex_tok.transform(df);

    result.show();
    SUCCEED();
}