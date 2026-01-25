# Spark Connect C++ spark

## Overview

This repository hosts a **native C++ spark** for **Apache Spark Connect**.

Spark Connect introduces a **decoupled spark-server architecture** for Apache Spark, enabling remote execution of Spark operations. This spark offers a **high-performance, idiomatic C++ interface** to Spark SQL, with efficient **Apache Arrow-based columnar serialization**.

* Status: _WIP_

---

## Getting Started

### 1. Prerequisites

- **Apache Spark 3.5+** with Spark Connect enabled
- **C++17 or later**
- Libraries:
  - `gRPC`
  - `Protobuf`
  - `Apache Arrow`
  - `uuid`

### 2. Build

```bash
make clean && make run
```

---

## Example Usage

```cpp
#include "client.h"
#include "config.h"

int main() {
    Config conf;
    conf.setHost("localhost").setPort(15002);
    SparkSession spark(conf);

    auto df = spark.sql("SELECT * FROM range(100)");
    df.show(5);
}
```

---

## Showcase: Supported Data Types

### Basic Integers

```cpp
auto df1 = spark.sql("SELECT * FROM range(1000)");
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

---

### String Column

```cpp
auto df2 = spark.sql("SELECT 'John' AS name");
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

---

### Mixed Types

```cpp
auto df3 = spark.sql(R"(
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

---

### Decimal, Date, Timestamp

```cpp
auto df4 = spark.sql(R"(
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

---

### Date and Timestamp Ranges

```cpp
auto df5 = spark.sql(R"(
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

### Nulls and Booleans

```cpp
auto df6 = spark.sql(R"(
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

---

### Float, Double, Binary

```cpp
auto df7 = spark.sql(R"(
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

---

## API Overview

### Connecting to Spark

```cpp
Config config;
config.setHost("remote-spark-host").setPort(15002);
SparkSession spark(config);
```

### Executing SQL

```cpp
auto df = spark.sql("SELECT name, age FROM people");
df.show(10);
```

### Programmatic Ranges

```cpp
auto df = spark.range(100);
df.show();
```

## DataFrame API

### Retrieve Column Names

```cpp
auto df = spark.sql("SELECT age, name, state FROM people");
auto cols = df.columns();
// Returns: {"age", "name", "state"}
```

### Example: Filter Columns

```cpp
auto df = spark.sql("SELECT * FROM table");
auto cols = df.columns();

// Select all columns except 'age'
std::vector<std::string> selected;
for (const auto& col : cols) {
    if (col != "age") selected.push_back(col);
}
```

### Example: Check Column Existence

```cpp
auto cols = df.columns();
bool has_state = std::find(cols.begin(), cols.end(), "state") != cols.end();
```

### Example: Schema Comparison

```cpp
bool same_schema = df1.columns() == df2.columns();
```

---

## License

Apache 2.0
