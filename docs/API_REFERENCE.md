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
| Daniel      | 36  | 115000 |
| Isabella    | 26  | 88000  |
| William     | 38  | 124000 |
| Mia         | 24  | 76000  |
| Benjamin    | 33  | 108000 |
| Charlotte   | 32  | 94000  |
| Lucas       | 41  | 130000 |
| Amelia      | 23  | 75000  |
| Henry       | 39  | 125000 |
| Harper      | 35  | 102000 |
| Elijah      | 42  | 135000 |
| Evelyn      | 28  | 87000  |
| Alexander   | 37  | 118000 |
| Abigail     | 30  | 92000  |
| Mason       | 43  | 138000 |
| Ella        | 25  | 83000  |
| Logan       | 27  | 96000  |
| Grace       | 26  | 91000  |
| Jackson     | 29  | 101000 |
| Chloe       | 31  | 93000  |
| Sebastian   | 33  | 106000 |
| Lily        | 34  | 95000  |
| Jack        | 38  | 123000 |
| Aria        | 22  | 71000  |
| Owen        | 36  | 117000 |
| Scarlett    | 28  | 97000  |
| Samuel      | 40  | 127000 |
| Zoe         | 24  | 78000  |
| Matthew     | 41  | 129000 |
| Nora        | 27  | 88000  |
| Joseph      | 45  | 136000 |
| Riley       | 25  | 82000  |
| Levi        | 35  | 112000 |
| Victoria    | 29  | 99000  |
| Mateo       | 32  | 104000 |
| Hannah      | 26  | 90000  |
| Aiden       | 39  | 126000 |
| Luna        | 23  | 77000  |
| Ethan       | 44  | 134000 |
| Penelope    | 30  | 94000  |
| Logan       | 37  | 119000 |
| Camila      | 28  | 89000  |
| Jayden      | 31  | 108000 |
| Layla       | 33  | 95000  |
| Grayson     | 38  | 121000 |
| Mila        | 24  | 76000  |
| Carter      | 42  | 133000 |
| Ellie       | 25  | 81000  |
| Julian      | 34  | 114000 |
| Addison     | 29  | 98000  |
| Hudson      | 40  | 128000 |
| Avery       | 26  | 88000  |
| Dylan       | 27  | 97000  |
| Sofia       | 32  | 93000  |
| Lincoln     | 35  | 111000 |
| Madison     | 28  | 91000  |
| Ezra        | 36  | 116000 |
| Hazel       | 30  | 95000  |
| Isaiah      | 41  | 130000 |
| Lillian     | 23  | 74000  |
| Thomas      | 37  | 118000 |
| Zoey        | 25  | 86000  |
| Charles     | 43  | 137000 |
| Nora        | 27  | 89000  |
| Christopher | 39  | 124000 |
| Violet      | 31  | 94000  |
| Josiah      | 40  | 128000 |
| Aurora      | 26  | 90000  |
| Andrew      | 33  | 106000 |
| Hannah      | 29  | 97000  |
| Nathan      | 35  | 113000 |
| Stella      | 24  | 79000  |
| Adrian      | 32  | 105000 |
| Paisley     | 28  | 93000  |
| Christian   | 38  | 122000 |
| Eleanor     | 27  | 94000  |
| Colton      | 36  | 117000 |
| Skylar      | 25  | 88000  |
| Cameron     | 34  | 110000 |
| Lucy        | 31  | 92000  |
| Aaron       | 40  | 129000 |
| Savannah    | 30  | 97000  |
| Eli         | 29  | 98000  |
| Brooklyn    | 23  | 75000  |
| Connor      | 39  | 126000 |
| Bella       | 28  | 91000  |
| Landon      | 33  | 109000 |
| Claire      | 27  | 90000  |
| Jonathan    | 41  | 132000 |
| Audrey      | 25  | 87000  |
| Nolan       | 35  | 115000 |
| Samantha    | 26  | 89000  |
| Easton      | 38  | 120000 |
| Allison     | 24  | 83000  |
| Dominic     | 37  | 119000 |
| Anna        | 30  | 95000  |
| Hunter      | 42  | 134000 |
| Leah        | 29  | 94000  |
| Brayden     | 31  | 107000 |
| Lucy        | 28  | 93000  |
| Ezekiel     | 39  | 125000 |
| Sarah       | 27  | 91000  |
| Angel       | 36  | 113000 |
| Aaliyah     | 24  | 78000  |
| Miles       | 34  | 112000 |
| Cora        | 25  | 85000  |
| Robert      | 46  | 138000 |
| Natalie     | 28  | 96000  |
| Parker      | 40  | 127000 |
| Lydia       | 23  | 77000  |
| Ian         | 33  | 109000 |
| Kennedy     | 27  | 92000  |
| Adam        | 38  | 121000 |
| Ruby        | 25  | 86000  |
| Jaxon       | 31  | 105000 |
| Caroline    | 26  | 89000  |
| Greyson     | 35  | 114000 |
| Alice       | 30  | 94000  |
| Asher       | 37  | 118000 |
| Sadie       | 28  | 92000  |
| Leo         | 32  | 107000 |
| Peyton      | 25  | 87000  |
| Josephine   | 27  | 91000  |
| Mateo       | 34  | 112000 |
| Delilah     | 24  | 79000  |
| Ryan        | 40  | 129000 |
| Madelyn     | 29  | 95000  |
| Anthony     | 41  | 131000 |
| Clara       | 23  | 77000  |
| Joshua      | 35  | 116000 |
| Emilia      | 26  | 89000  |
| Christopher | 38  | 124000 |
| Aubrey      | 28  | 91000  |
| Jack        | 42  | 133000 |
| Naomi       | 25  | 86000  |
| Maverick    | 30  | 104000 |
| Isla        | 24  | 78000  |
| Jameson     | 34  | 113000 |
| Ivy         | 27  | 93000  |
| Elias       | 37  | 119000 |
| Sophie      | 26  | 91000  |
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
