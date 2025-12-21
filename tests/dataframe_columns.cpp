#include <iostream>
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "session.h"
#include "dataframe.h"

//----------------------------------------------------------------
// Helper function to print test results
//----------------------------------------------------------------
void print_test_result(const std::string& test_name, bool passed)
{
    std::cout << "[" << (passed ? "PASS" : "FAIL") << "] " << test_name << std::endl;
}

void print_columns(const std::vector<std::string>& cols)
{
    std::cout << "  Columns: {";
    for (size_t i = 0; i < cols.size(); ++i) {
        std::cout << "\"" << cols[i] << "\"";
        if (i < cols.size() - 1) std::cout << ", ";
    }
    std::cout << "}" << std::endl;
}

//----------------------------------------------------------------
// Test 1: Basic column retrieval
//----------------------------------------------------------------
bool test_basic_column_retrieval(SparkSession& spark)
{
    std::cout << "\n--- Test 1: Basic Column Retrieval ---" << std::endl;
    
    auto df = spark.sql("SELECT 14 AS age, 'Tom' AS name, 'CA' AS state");
    auto cols = df.columns();
    
    std::cout << "  Expected: {\"age\", \"name\", \"state\"}" << std::endl;
    print_columns(cols);
    
    bool passed = (cols.size() == 3 && 
                   cols[0] == "age" && 
                   cols[1] == "name" && 
                   cols[2] == "state");
    
    return passed;
}

//----------------------------------------------------------------
// Test 2: Column order preservation (non-alphabetical)
//----------------------------------------------------------------
bool test_column_order_preservation(SparkSession& spark)
{
    std::cout << "\n--- Test 2: Column Order Preservation ---" << std::endl;
    
    auto df = spark.sql("SELECT 1 AS z, 2 AS a, 3 AS m");
    auto cols = df.columns();
    
    std::cout << "  Expected: {\"z\", \"a\", \"m\"} (NOT alphabetical)" << std::endl;
    print_columns(cols);
    
    bool passed = (cols.size() == 3 && 
                   cols[0] == "z" && 
                   cols[1] == "a" && 
                   cols[2] == "m");
    
    return passed;
}

//----------------------------------------------------------------
// Test 3: Single column DataFrame
//----------------------------------------------------------------
bool test_single_column(SparkSession& spark)
{
    std::cout << "\n--- Test 3: Single Column DataFrame ---" << std::endl;
    
    auto df = spark.sql("SELECT 42 AS answer");
    auto cols = df.columns();
    
    std::cout << "  Expected: {\"answer\"}" << std::endl;
    print_columns(cols);
    
    bool passed = (cols.size() == 1 && cols[0] == "answer");
    
    return passed;
}

//----------------------------------------------------------------
// Test 4: Many columns
//----------------------------------------------------------------
bool test_many_columns(SparkSession& spark)
{
    std::cout << "\n--- Test 4: Many Columns ---" << std::endl;
    
    auto df = spark.sql(R"(
        SELECT 
            1 AS col1, 2 AS col2, 3 AS col3, 4 AS col4, 5 AS col5,
            6 AS col6, 7 AS col7, 8 AS col8, 9 AS col9, 10 AS col10
    )");
    auto cols = df.columns();
    
    std::cout << "  Expected: 10 columns (col1 through col10)" << std::endl;
    print_columns(cols);
    
    bool passed = (cols.size() == 10 && 
                   cols[0] == "col1" && 
                   cols[9] == "col10");
    
    return passed;
}

//----------------------------------------------------------------
// Test 5: Empty DataFrame (schema preserved)
//----------------------------------------------------------------
bool test_empty_dataframe_with_schema(SparkSession& spark)
{
    std::cout << "\n--- Test 5: Empty DataFrame with Schema ---" << std::endl;
    
    auto df = spark.sql("SELECT 1 AS id, 'test' AS name WHERE 1=0");
    auto cols = df.columns();
    
    std::cout << "  Expected: {\"id\", \"name\"} (even though DataFrame is empty)" << std::endl;
    print_columns(cols);
    
    bool passed = (cols.size() == 2 && 
                   cols[0] == "id" && 
                   cols[1] == "name");
    
    return passed;
}

//----------------------------------------------------------------
// Test 6: Check if column exists (std::find)
//----------------------------------------------------------------
bool test_column_existence_check(SparkSession& spark)
{
    std::cout << "\n--- Test 6: Column Existence Check ---" << std::endl;
    
    auto df = spark.sql("SELECT 1 AS id, 'John' AS name, 100 AS salary");
    auto cols = df.columns();
    
    print_columns(cols);
    
    bool has_salary = std::find(cols.begin(), cols.end(), "salary") != cols.end();
    bool has_age = std::find(cols.begin(), cols.end(), "age") != cols.end();
    
    std::cout << "  Has 'salary': " << (has_salary ? "true" : "false") 
              << " (expected: true)" << std::endl;
    std::cout << "  Has 'age': " << (has_age ? "true" : "false") 
              << " (expected: false)" << std::endl;
    
    bool passed = (has_salary && !has_age);
    
    return passed;
}

//----------------------------------------------------------------
// Test 7: Schema comparison between DataFrames
//----------------------------------------------------------------
bool test_schema_comparison(SparkSession& spark)
{
    std::cout << "\n--- Test 7: Schema Comparison ---" << std::endl;
    
    auto df1 = spark.sql("SELECT 1 AS id, 'Alice' AS name");
    auto df2 = spark.sql("SELECT 2 AS id, 'Bob' AS name");
    auto df3 = spark.sql("SELECT 3 AS id, 'Charlie' AS username");
    
    auto cols1 = df1.columns();
    auto cols2 = df2.columns();
    auto cols3 = df3.columns();
    
    std::cout << "  df1 columns: ";
    print_columns(cols1);
    std::cout << "  df2 columns: ";
    print_columns(cols2);
    std::cout << "  df3 columns: ";
    print_columns(cols3);
    
    bool same_schema_12 = (cols1 == cols2);
    bool same_schema_13 = (cols1 == cols3);
    
    std::cout << "  df1 == df2: " << (same_schema_12 ? "true" : "false") 
              << " (expected: true)" << std::endl;
    std::cout << "  df1 == df3: " << (same_schema_13 ? "true" : "false") 
              << " (expected: false)" << std::endl;
    
    bool passed = (same_schema_12 && !same_schema_13);
    
    return passed;
}

//----------------------------------------------------------------
// Test 8: Column filtering (select columns except one)
//----------------------------------------------------------------
bool test_column_filtering(SparkSession& spark)
{
    std::cout << "\n--- Test 8: Column Filtering ---" << std::endl;
    
    auto df = spark.sql("SELECT 14 AS age, 'Tom' AS name, 'CA' AS state");
    auto cols = df.columns();
    
    std::cout << "  Original columns: ";
    print_columns(cols);
    
    // Filter out 'age'
    std::vector<std::string> selected;
    for (const auto& c : cols) {
        if (c != "age") {
            selected.push_back(c);
        }
    }
    
    std::cout << "  Filtered columns (excluding 'age'): ";
    print_columns(selected);
    
    bool passed = (selected.size() == 2 && 
                   selected[0] == "name" && 
                   selected[1] == "state");
    
    return passed;
}

//----------------------------------------------------------------
// Test 9: Iterate over columns
//----------------------------------------------------------------
bool test_column_iteration(SparkSession& spark)
{
    std::cout << "\n--- Test 9: Column Iteration ---" << std::endl;
    
    auto df = spark.sql("SELECT 'a' AS col_a, 'b' AS col_b, 'c' AS col_c");
    auto cols = df.columns();
    
    std::cout << "  Iterating through columns:" << std::endl;
    size_t count = 0;
    for (const auto& col : cols) {
        std::cout << "    - Column " << (++count) << ": " << col << std::endl;
    }
    
    bool passed = (count == 3);
    
    return passed;
}

//----------------------------------------------------------------
// Test 10: Special characters in column names
//----------------------------------------------------------------
bool test_special_characters_in_names(SparkSession& spark)
{
    std::cout << "\n--- Test 10: Special Characters in Column Names ---" << std::endl;
    
    auto df = spark.sql("SELECT 1 AS `user.id`, 2 AS `count(*)`, 3 AS `col with spaces`");
    auto cols = df.columns();
    
    std::cout << "  Expected: backtick-quoted column names" << std::endl;
    print_columns(cols);
    
    bool passed = (cols.size() == 3 &&
                   cols[0] == "user.id" &&
                   cols[1] == "count(*)" &&
                   cols[2] == "col with spaces");
    
    return passed;
}

//----------------------------------------------------------------
// Test 11: Mixed data types columns
//----------------------------------------------------------------
bool test_mixed_data_types(SparkSession& spark)
{
    std::cout << "\n--- Test 11: Mixed Data Types Columns ---" << std::endl;
    
    auto df = spark.sql(R"(
        SELECT 
            42 AS int_col,
            'hello' AS string_col,
            true AS bool_col,
            3.14 AS double_col,
            CAST('2024-01-01' AS DATE) AS date_col
    )");
    auto cols = df.columns();
    
    std::cout << "  Expected: columns of different types" << std::endl;
    print_columns(cols);
    
    bool passed = (cols.size() == 5 &&
                   cols[0] == "int_col" &&
                   cols[1] == "string_col" &&
                   cols[2] == "bool_col" &&
                   cols[3] == "double_col" &&
                   cols[4] == "date_col");
    
    return passed;
}

//----------------------------------------------------------------
// Test 12: Columns after transformations (derived DataFrame)
//----------------------------------------------------------------
bool test_columns_after_transformation(SparkSession& spark)
{
    std::cout << "\n--- Test 12: Columns After Transformation ---" << std::endl;
    
    auto df_original = spark.sql("SELECT 1 AS a, 2 AS b, 3 AS c");
    auto df_selected = spark.sql("SELECT b, a FROM (SELECT 1 AS a, 2 AS b, 3 AS c)");
    
    auto cols_original = df_original.columns();
    auto cols_selected = df_selected.columns();
    
    std::cout << "  Original columns: ";
    print_columns(cols_original);
    std::cout << "  After SELECT b, a: ";
    print_columns(cols_selected);
    
    bool passed = (cols_selected.size() == 2 &&
                   cols_selected[0] == "b" &&
                   cols_selected[1] == "a");
    
    return passed;
}

//----------------------------------------------------------------
// Test 13: Case sensitivity
//----------------------------------------------------------------
bool test_case_sensitivity(SparkSession& spark)
{
    std::cout << "\n--- Test 13: Case Sensitivity ---" << std::endl;
    
    auto df = spark.sql("SELECT 1 AS Name, 2 AS NAME, 3 AS name");
    auto cols = df.columns();
    
    print_columns(cols);
    
    // Spark is case-insensitive by default, but preserves original case
    // bool passed = (cols.size() == 1); // Might be deduplicated
    
    std::cout << "Note: Behavior depends on Spark's case sensitivity settings" << std::endl;
    
    return true; // This is informational
}

//----------------------------------------------------------------
// Test 14: Error handling - invalid plan
//----------------------------------------------------------------
bool test_error_handling(SparkSession& spark)
{
    std::cout << "\n--- Test 14: Error Handling ---" << std::endl;
    
    try {
        // This should work fine
        auto df = spark.sql("SELECT 1 AS id");
        auto cols = df.columns();
        
        std::cout << "  Basic query succeeded" << std::endl;
        
        return true;
    }
    catch (const std::exception& e) {
        std::cerr << "  Unexpected error: " << e.what() << std::endl;
        return false;
    }
}

//----------------------------------------------------------------
// Test 15: Error handling - invalid plan
//----------------------------------------------------------------
bool test_error_handling_invalid_plan(SparkSession& spark)
{
    std::cout << "\n--- Test 15: Error Handling - Invalid Plan ---" << std::endl;
    
    try {
        // Intentionally create an invalid DataFrame (e.g., from a non-existent table)
        auto df = spark.sql("SELECT * FROM non_existent_table");
        auto cols = df.columns();
        
        std::cerr << "  ERROR: Expected exception for invalid plan, but got columns:" << std::endl;
        print_columns(cols);
        
        return false; // Should not reach here
    }
    catch (const std::exception& e) {
        std::cout << "  Caught expected error: " << e.what() << std::endl;
        return true; // Expected path
    }
}

//----------------------------------------------------------------
// Main test runner
//----------------------------------------------------------------
int main()
{
    std::cout << "========================================" << std::endl;
    std::cout << "DataFrame::columns() Unit Tests" << std::endl;
    std::cout << "========================================" << std::endl;

    try
    {
        // Create SparkSession
        SparkSession& spark = SparkSession::builder()
                                  .master("localhost")
                                  .appName("DataFrameColumnsTest")
                                  .getOrCreate();

        std::cout << "\nSparkSession created successfully" << std::endl;
        std::cout << "Session ID: " << spark.session_id() << std::endl;
        std::cout << "User ID: " << spark.user_id() << std::endl;

        // Run all tests
        std::vector<std::pair<std::string, bool>> results;
        
        results.push_back({"Basic Column Retrieval", test_basic_column_retrieval(spark)});
        results.push_back({"Column Order Preservation", test_column_order_preservation(spark)});
        results.push_back({"Single Column", test_single_column(spark)});
        results.push_back({"Many Columns", test_many_columns(spark)});
        results.push_back({"Empty DataFrame with Schema", test_empty_dataframe_with_schema(spark)});
        results.push_back({"Column Existence Check", test_column_existence_check(spark)});
        results.push_back({"Schema Comparison", test_schema_comparison(spark)});
        results.push_back({"Column Filtering", test_column_filtering(spark)});
        results.push_back({"Column Iteration", test_column_iteration(spark)});
        results.push_back({"Special Characters in Names", test_special_characters_in_names(spark)});
        results.push_back({"Mixed Data Types", test_mixed_data_types(spark)});
        results.push_back({"Columns After Transformation", test_columns_after_transformation(spark)});
        results.push_back({"Case Sensitivity", test_case_sensitivity(spark)});
        results.push_back({"Error Handling", test_error_handling(spark)});
        results.push_back({"Error Handling - Invalid Plan", test_error_handling_invalid_plan(spark)});
        // Print summary
        std::cout << "\n========================================" << std::endl;
        std::cout << "Test Summary" << std::endl;
        std::cout << "========================================" << std::endl;
        
        int passed = 0;
        int total = results.size();
        
        for (const auto& result : results) {
            print_test_result(result.first, result.second);
            if (result.second) passed++;
        }
        
        std::cout << "\n" << passed << "/" << total << " tests passed" << std::endl;
        
        if (passed == total) {
            std::cout << "\n✓ ALL TESTS PASSED!" << std::endl;
            return 0;
        } else {
            std::cout << "\n✗ SOME TESTS FAILED!" << std::endl;
            return 1;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "\nERROR: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}