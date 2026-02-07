# Spark Connect C++ API Reference

## Initializing a Spark Session

```cpp
#include "client.h"
#include "config.h"

int main() {
    Config conf;
    conf.setHost("localhost").setPort(15002);
    // conf.setHost("123.45.67.8").setPort(15002);
    SparkSession spark(conf);

    auto df = spark->sql("SELECT * FROM range(100)");
    df.show(5);
}
```

## DataFrame API

### Range Selection

```cpp
auto df1 = spark->sql("SELECT * FROM range(1000)");
df1.show(5);
```

**Output:**

```
+-----+
| id  |
+-----+
| 0   |
| 1   |
| 2   |
| 3   |
| 4   |
+-----+
```

### String Selection

```cpp
auto df2 = spark->sql("SELECT 'John' AS name");
df2.show();
```

**Output:**

```
+------+
| name |
+------+
| John |
+------+
```

### Mixed Types

```cpp
auto df3 = spark->sql(R"(
    SELECT id,
           CASE WHEN id % 2 = 0 THEN 'Alice' ELSE 'Bob' END AS name,
           id * 1.5 AS score,
           id % 3 = 0 AS is_active
    FROM range(10)
)");
df3.show(10);
```

**Output:**

```
+----+-------+-------+-----------+
| id | name  | score | is_active|
+----+-------+-------+-----------+
| 0  | Alice | 0.0   | true      |
| 1  | Bob   | 1.5   | false     |
| 2  | Alice | 3.0   | false     |
| 3  | Bob   | 4.5   | true      |
| 4  | Alice | 6.0   | false     |
| 5  | Bob   | 7.5   | false     |
| 6  | Alice | 9.0   | true      |
| 7  | Bob   | 10.5  | false     |
| 8  | Alice | 12.0  | false     |
| 9  | Bob   | 13.5  | true      |
+----+-------+-------+-----------+
```

### Decimal, Date, Timestamp

```cpp
auto df4 = spark->sql(R"(
    SELECT
        CAST('2024-01-01' AS DATE) AS date_col,
        CAST('2024-01-01 12:34:56' AS TIMESTAMP) AS ts_col,
        CAST(12345.6789 AS DECIMAL(20, 4)) AS decimal_col
)");
df4.show();
```

**Output:**

```
+------------+-----------------------+-------------+
| date_col   | ts_col                | decimal_col |
+------------+-----------------------+-------------+
| 2024-01-01 | 2024-01-01 12:34:56   | 12345.6789  |
+------------+-----------------------+-------------+
```

### Date and Timestamp Ranges

```cpp
auto df5 = spark->sql(R"(
    SELECT
        CAST(date_sub(current_date(), CAST(id AS INT)) AS DATE) AS date32_col,
        CAST(date_add(current_timestamp(), CAST(id AS INT)) AS TIMESTAMP) AS ts_col
    FROM range(5)
)");
df5.show();
```

**Output:**

```
+-------------+-------------------------------+
| date32_col  | ts_col                        |
+-------------+-------------------------------+
| 2024-08-02  | 2024-08-02T15:00:00           |
| 2024-08-01  | 2024-08-03T15:00:00           |
| 2024-07-31  | 2024-08-04T15:00:00           |
| 2024-07-30  | 2024-08-05T15:00:00           |
| 2024-07-29  | 2024-08-06T15:00:00           |
+-------------+-------------------------------+
```

---

### Null and Boolean Logic

```cpp
auto df6 = spark->sql(R"(
    SELECT
        IF(id % 2 = 0, null, id) AS maybe_null,
        id % 2 = 0 AS is_even
    FROM range(6)
)");
df6.show();
```

**Output:**

```
+------------+---------+
| maybe_null | is_even |
+------------+---------+
| null       | true    |
| 1          | false   |
| null       | true    |
| 3          | false   |
| null       | true    |
| 5          | false   |
+------------+---------+
```

### Float, Double, Binary (Numeric Types)

```cpp
auto df7 = spark->sql(R"(
    SELECT
        CAST(id AS FLOAT) / 3.0 AS float_val,
        CAST(id AS DOUBLE) * 2.5 AS double_val,
        encode(CAST(id AS STRING), 'utf-8') AS bin_val
    FROM range(5)
)");
df7.show();
```

**Output:**

```
+-----------+------------+---------+
| float_val | double_val | bin_val |
+-----------+------------+---------+
| 0.0       | 0.0        | 30      |
| 0.3333    | 2.5        | 31      |
| 0.6666    | 5.0        | 32      |
| 1.0       | 7.5        | 33      |
| 1.3333    | 10.0       | 34      |
+-----------+------------+---------+
```

### Schema Introspection

* The following snippet showcases `df.schema()`:

```cpp
    auto df = spark->sql(R"(
        SELECT *
                FROM VALUES
                    (14, 'Tom'),
                    (14, 'Alice'),
                    (14, 'Bob')
                AS people(age, name)
    )");

    std::string schema_json = df.schema().json();

    std::cout << "StructType([" << schema_json << "])" << std::endl;

    // --------------------------------------------------------------------------------------
    // The following gets printed to the console
    //
    // StructType([{"type":"struct","fields":[{"name":"age","type":"integer","nullable":false,"metadata":{}},{"name":"name","type":"string","nullable":false,"metadata":{}}]}])
    // --------------------------------------------------------------------------------------
```

* The following snippet showcases `df.printSchema()`:

```cpp
auto df = spark->sql("SELECT CAST(1 AS INT) AS id, array(CAST(1.1 AS DOUBLE), CAST(2.2 AS DOUBLE)) AS vals");

df.printSchema();

// --------------------------------------------------------------------------------------
// The following gets printed to the console
// 
// root
//  |-- id: integer (nullable = false)
//  |-- vals: array
//  |   |-- element: double (nullable = false)
// --------------------------------------------------------------------------------------

```

### Head

```cpp
auto df = spark->sql("SELECT 1 AS a, 'Alice' AS b");

auto rows = df.head(1);
```

**Output:**

```
Row(a=1, b='Alice')
```

### Executing SQL

```cpp
auto df = spark->sql("SELECT name, age FROM people");
df.show(10);
```

### Programmatic Ranges

```cpp
auto df = spark.range(100);
df.show();
```

### Retrieve Column Names

```cpp
auto df = spark->sql("SELECT age, name, state FROM people");
auto cols = df.columns();
// Returns: {"age", "name", "state"}
```

### Filter Columns

```cpp
auto df = spark->sql("SELECT * FROM table");
auto cols = df.columns();

// Select all columns except 'age'
std::vector<std::string> selected;
for (const auto& col : cols) {
    if (col != "age") selected.push_back(col);
}
```

### Check Column Existence

```cpp
auto cols = df.columns();
bool has_state = std::find(cols.begin(), cols.end(), "state") != cols.end();
```

### Schema Comparison

```cpp
bool same_schema = df1.columns() == df2.columns();
```

---

## DataFrame Reader

### Read Parquet files

```cpp
auto df = spark->read().parquet("datasets/flights.parquet");
df.show(10);

// ----------------------------------------------------------------------------
// Some Common Parquet options:
//
// "mergeSchema" - Merges schemas across all Parquet part-files which results in expensive operations
// "datetimeRebaseMode" - Handles legacy dates from older Spark versions
// ----------------------------------------------------------------------------

// auto df = spark->read()
//                 .option("mergeSchema", "true")
//                 .parquet("datasets/flights.parquet");
```

**Output:**

```
+------------+---------------+---------------+---------------+---------------+-----------+-----------+
| FL_DATE    | DEP_DELAY     | ARR_DELAY     | AIR_TIME      | DISTANCE      | DEP_TIME  | ARR_TIME  |
+------------+---------------+---------------+---------------+---------------+-----------+-----------+
| 2006-01-01 | (unsupported) | (unsupported) | (unsupported) | (unsupported) | 9.083333  | 12.483334 |
| 2006-01-02 | (unsupported) | (unsupported) | (unsupported) | (unsupported) | 11.783334 | 15.766666 |
| 2006-01-03 | (unsupported) | (unsupported) | (unsupported) | (unsupported) | 8.883333  | 12.133333 |
| 2006-01-04 | (unsupported) | (unsupported) | (unsupported) | (unsupported) | 8.916667  | 11.950000 |
| 2006-01-05 | (unsupported) | (unsupported) | (unsupported) | (unsupported) | 8.950000  | 11.883333 |
| 2006-01-06 | (unsupported) | (unsupported) | (unsupported) | (unsupported) | 8.933333  | 11.633333 |
| 2006-01-08 | (unsupported) | (unsupported) | (unsupported) | (unsupported) | 8.950000  | 12.133333 |
| 2006-01-09 | (unsupported) | (unsupported) | (unsupported) | (unsupported) | 9.050000  | 12.166667 |
| 2006-01-10 | (unsupported) | (unsupported) | (unsupported) | (unsupported) | 8.883333  | 11.816667 |
| 2006-01-11 | (unsupported) | (unsupported) | (unsupported) | (unsupported) | 9.133333  | 12.000000 |
+------------+---------------+---------------+---------------+---------------+-----------+-----------+
```

### Read JSON files

```cpp
auto df = spark->read().json("datasets/people.json");
df.show();
```

**Output:**

```
+-----+---------+--------+
| age | name    | salary |
+-----+---------+--------+
| 25  | John    | 100000 |
| 30  | Alice   | 85000  |
| 45  | Robert  | 120000 |
| 28  | Mary    | 95000  |
| 34  | David   | 110000 |
| 22  | Sophia  | 72000  |
| 40  | James   | 132000 |
| 27  | Emily   | 89000  |
| 31  | Michael | 99000  |
| 29  | Olivia  | 97000  |
+-----+---------+--------+
```

### Read CSV files

```cpp
auto df = spark->read().csv("datasets/people.csv");
df.show(163);
```

**Output:**

```
+-------------+-----+--------+
| _c0         | _c1 | _c2    |
+-------------+-----+--------+
| name        | age | salary |
| John        | 25  | 100000 |
| Alice       | 30  | 85000  |
| Robert      | 45  | 120000 |
| Mary        | 28  | 95000  |
| David       | 34  | 110000 |
| Sophia      | 22  | 72000  |
| James       | 40  | 132000 |
| Emily       | 27  | 89000  |
| Michael     | 31  | 99000  |
| Olivia      | 29  | 97000  |

--- Truncated for brevity ---

| Gavin       | 39  | 126000 |
| Serenity    | 28  | 92000  |
| Caleb       | 33  | 110000 |
| Ariana      | 25  | 88000  |
| William     | 45  | 135000 |
| Hailey      | 24  | 80000  |
| Luke        | 31  | 107000 |
| Kennedy     | 29  | 94000  |
| Isaac       | 36  | 115000 |
| Sarah       | 27  | 92000  |
| Jayden      | 38  | 122000 |
+-------------+-----+--------+
```

## DataFrame Writer

### Write Parquet files

```cpp
auto write_data = spark->range(100);

write_data.filter("id > 50")
    .write()
    .mode("overwrite")
    .option("compression", "gzip")
    .parquet("output/range_data.parquet");

auto read_data = spark->read().parquet("output/range_data.parquet");
read_data.show(100);
```

**Output:**

```
+----------------------+
| id                   |
+----------------------+
| 75                   |
| 76                   |
| 77                   |
| 78                   |
| 79                   |
| 80                   |

--- Truncated for brevity ---

| 67                   |
| 68                   |
| 69                   |
| 70                   |
| 71                   |
| 72                   |
| 73                   |
| 74                   |
+----------------------+
```


## Aggregate Functions

### Row Count

```cpp
auto df = spark->range(1000);
auto row_count = df.count();
```

## Filtering

### Select Subset

```cpp
auto df = spark->sql(R"(SELECT 1 AS a, 2 AS b, 3 AS c)");

// ------------------------------------------------------------
// Select only 'b' and 'a' (Reordering them)
// ------------------------------------------------------------
auto df_subset = df.select({"b", "a"});

df_subset.show();

```

**Output:**

```
+---+---+
| b | a |
+---+---+
| 2 | 1 |
+---+---+
```

### Filter

```cpp
auto df = spark->read().csv("datasets/peaople.csv");
auto filtered_df = df.filter("age > 30");
filtered_df.show();
```

**Output:**

```
+----------------------+----------------------+----------------------+
| name                 | age                  | salary               |
+----------------------+----------------------+----------------------+
| Robert               | 45                   | 120000               |
| David                | 34                   | 110000               |
| James                | 40                   | 132000               |
| Michael              | 31                   | 99000                |
| Daniel               | 36                   | 115000               |
| William              | 38                   | 124000               |
| Benjamin             | 33                   | 108000               |
| Charlotte            | 32                   | 94000                |
| Lucas                | 41                   | 130000               |
| Henry                | 39                   | 125000               |
+----------------------+----------------------+----------------------+
```

#### Complex Filtering

```cpp
auto df = spark->read().csv("datasets/people.csv");
auto complex_logic = df.filter("age > 20 AND name LIKE 'J%' OR salary = 130000");
complex_logic.show();
```

**Output:**

```
+----------------------+----------------------+----------------------+
| name                 | age                  | salary               |
+----------------------+----------------------+----------------------+
| John                 | 25                   | 100000               |
| James                | 40                   | 132000               |
| Lucas                | 41                   | 130000               |
| Jackson              | 29                   | 101000               |
| Jack                 | 38                   | 123000               |
| Joseph               | 45                   | 136000               |
| Jayden               | 31                   | 108000               |
| Julian               | 34                   | 114000               |
| Isaiah               | 41                   | 130000               |
| Josiah               | 40                   | 128000               |
+----------------------+----------------------+----------------------+
```

### Where (Filter Alias)

```cpp
auto df = spark->read().csv("datasets/people.csv");
auto filtered_df = df.where("age < 25");
filtered_df.show();
```

**Output:**

```
+----------------------+----------------------+----------------------+
| name                 | age                  | salary               |
+----------------------+----------------------+----------------------+
| Sophia               | 22                   | 72000                |
| Mia                  | 24                   | 76000                |
| Amelia               | 23                   | 75000                |
| Aria                 | 22                   | 71000                |
| Zoe                  | 24                   | 78000                |
| Luna                 | 23                   | 77000                |
| Mila                 | 24                   | 76000                |
| Lillian              | 23                   | 74000                |
| Stella               | 24                   | 79000                |
| Brooklyn             | 23                   | 75000                |
+----------------------+----------------------+----------------------+
```