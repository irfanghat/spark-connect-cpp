#include "session.h"
#include "config.h"

int main()
{
    Config conf;
    conf.setHost("localhost").setPort(15002);

    SparkSession client(conf);

    //------------------------
    // Basic int
    //------------------------
    auto df1 = client.sql("SELECT * FROM range(1000)");
    df1.show(5);
    
    //------------------------
    // Simple string
    //------------------------
    auto df2 = client.sql("SELECT 'John' AS name");
    df2.show();
    
    //------------------------
    // Mixed types
    //------------------------
    auto df3 = client.sql(R"(
        SELECT id, 
               CASE WHEN id % 2 = 0 THEN 'Alice' ELSE 'Bob' END AS name,
               id * 1.5 AS score,
               id % 3 = 0 AS is_active
        FROM range(10)
    )");
    df3.show(20);

    //-----------------------------
    // Decimal + Date + Timestamp
    //-----------------------------
    auto df4 = client.sql(R"(
        SELECT 
            CAST('2024-01-01' AS DATE) AS date_col,
            CAST('2024-01-01 12:34:56' AS TIMESTAMP) AS ts_col,
            CAST(12345.6789 AS DECIMAL(20, 4)) AS decimal_col
    )");
    df4.show();

    //-----------------------------
    // Date ranges
    //-----------------------------
    auto df5 = client.sql(R"(
        SELECT 
            CAST(date_sub(current_date(), CAST(id AS INT)) AS DATE) AS date32_col,
            CAST(date_add(current_timestamp(), CAST(id AS INT)) AS TIMESTAMP) AS ts_col
        FROM range(5)
    )");
    df5.show();


    //-----------------------------
    // Null and boolean logic
    //-----------------------------
    auto df6 = client.sql(R"(
        SELECT 
            IF(id % 2 = 0, null, id) AS maybe_null,
            id % 2 = 0 AS is_even
        FROM range(6)
    )");
    df6.show();

    //-----------------------------
    // Float, double, binary
    //-----------------------------
    auto df7 = client.sql(R"(
        SELECT 
            CAST(id AS FLOAT) / 3.0 AS float_val,
            CAST(id AS DOUBLE) * 2.5 AS double_val,
            encode(CAST(id AS STRING), 'utf-8') AS bin_val
        FROM range(5)
    )");
    df7.show();
}
