# Spark Connect C++ Client

## Overview

This repository hosts a native C++ client for **Apache Spark Connect**. Spark Connect introduces a decoupled client-server architecture for Apache Spark, allowing remote execution of Spark operations.

This C++ client provides a high-performance interface for building Spark applications in C++, leveraging efficient data transfer through **Apache Arrow**.

The goal is to offer a robust, idiomatic C++ experience for interacting with Spark—suitable for performance-critical applications and integration into existing C++ ecosystems.

---

## Features

- **Native C++ Client**  
  A dedicated client implementation for Spark Connect, designed for performance and direct integration into C++ projects.

- **Apache Arrow Integration**  
  Seamless deserialization of Apache Arrow data streams received from Spark Connect responses, enabling efficient columnar data processing directly within your C++ application.

- **Modular Architecture**  
  Clean separation of logic for improved code organization, readability, and maintainability.

---

## Getting Started

To build and run the client, refer to our detailed setup guide:

- **[SETUP.md](SETUP.md)** – Provides comprehensive instructions for setting up your development environment, installing dependencies, building the client, and configuring VS Code.

---

## Usage Example

The `main.cpp` file provides a basic example demonstrating how to:

- Initialize the client
- Construct a simple Spark plan (e.g., a `RangeRelation`)
- Execute it
- Process the returned Apache Arrow `RecordBatch` data

### Example Snippet (from `main.cpp`)

```cpp
#include "client.h"
#include "spark/connect/base.pb.h"
#include "relations.h"

int main() {
    std::string server_address("localhost:15002");
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
    SparkConnectClient client(channel);

    spark::connect::UserContext user_context;
    // ... set user context ...

    std::string session_id = "your-session-id";

    std::shared_ptr<spark::client::RangeRelation> range_relation =
        spark::client::Range(10, std::optional<int64_t>{0}, std::optional<int64_t>{1}, std::optional<int32_t>{4});

    spark::connect::Plan plan;
    *plan.mutable_root() = range_relation->ToProto();

    std::vector<std::shared_ptr<arrow::RecordBatch>> results =
        client.ExecutePlan(plan, user_context, session_id);

    // Process results...
    return 0;
}