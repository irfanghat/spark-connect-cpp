# Spark Connect C++ Client

## Overview

This repository hosts a **native C++ client** for **Apache Spark Connect**.  
Spark Connect introduces a **decoupled client-server architecture** for Apache Spark, allowing remote execution of Spark operations.

This client provides a **high-performance, idiomatic C++ interface** for interacting with Spark, leveraging **Apache Arrow** for efficient columnar data transfer.

----------

## Features

**Native C++ Client** – Built from scratch for Spark Connect.  
**Arrow Integration** – Efficient Arrow-based serialization/deserialization.  

----------

## Getting Started

### **1. Prerequisites**

-   **Apache Spark 3.5/4.x** with Spark Connect enabled.
    
-   **C++17 or later**.
    
-   Dependencies:
    
    -   `gRPC`
    -   `Protobuf`
    -   `Apache Arrow`
    -   `uuid` (for session IDs)
        
### **2. Build**

```bash
make clean && make run
```

----------

## Usage Examples

Here’s how you can run a simple Spark query:

### **main.cpp**

```cpp
#include "client.h"
#include "config.h"
#include <iostream>

int main() {
    // Auto-managed config (similar to SparkSession.builder in PySpark)
    Config config;
    config.setHost("localhost").setPort(15002);

    SparkClient client(config);

    auto df = client.sql("SELECT * FROM range(5)");
    df.show();

    return 0;
}
```

### **Output**

```sh
id:   [
    0,
    1,
    2,
    3,
    4
  ]
id:   [
    0,
    0,
    0,
    0,
    0
  ]
```


```cpp
  auto df = client.sql("SELECT 'John' AS name");
  df.show();
```

### **Output**

```sh
name:   [
    "John"
  ]
```


----------

## API Overview

### **Creating a Client**

```cpp
Config config;
config.setHost("remote-spark-host").setPort(15002);
SparkClient client(config);
```

### **SQL Queries**

```cpp
auto df = client.sql("SELECT name, age FROM people");
df.show(20); // show first 20 rows
```

### **Programmatic Ranges**

```cpp
auto df = client.range(100);
df.show(5); // prints first 5 rows
```

_(More transformations like `select`, `filter`, `limit` are coming soon.)_

----------

## Roadmap

-   Pretty printing (PySpark-style)
-   Chained transformations (`df.select().filter().limit().show()`)
-   Full Arrow-to-Arrow zero-copy conversions for analytics workloads
-   Support for authentication and custom headers
-   Full test suite