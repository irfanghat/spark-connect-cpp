# Spark Connect C++

## Overview

This repository provides a **native C++ client for Apache Spark Connect**, enabling C++ applications to execute Spark SQL workloads against remote Spark clusters without requiring JVM dependencies.

Spark Connect introduces a **decoupled client–server architecture** for Apache Spark, where client applications construct logical query plans that are executed remotely by a Spark server. This project delivers an **idiomatic, high-performance C++ interface** for building and submitting Spark queries, with efficient **Apache Arrow–based columnar data exchange** over gRPC.

The library is intended for environments where:

* Native C++ integration is required
* JVM runtimes are impractical or undesirable
* High-throughput data movement is necessary
* Tight control over memory and system resources is important
* Existing performance-critical C++ systems need to integrate with Spark

**Status:** *Work in Progress*

![Architecture Diagram](https://github.com/irfanghat/spark-connect-cpp/blob/main/docs/ARICHITECTURE_DIAGRAM.png)

---

## Design Goals

* Native-first Spark integration for C++ ecosystems
* Efficient Arrow-based columnar data transfer
* Clear separation between client logic and remote Spark execution
* Compatibility with evolving Spark Connect protocols
* Predictable performance characteristics suitable for production systems
* Minimal runtime dependencies outside standard native infrastructure

---

## Use Cases

The Spark Connect C++ client is designed for environments that require **native system integration**, **efficient columnar data transfer**, and **low-overhead communication** with remote Spark clusters.

### Native Data Producers and High-Throughput Pipelines

Enable high-performance C++ services to submit ingestion workloads and push structured datasets to Spark using Apache Arrow serialization without JVM dependencies.

### Integration with Legacy, Industrial, and Scientific Systems

Connect existing C++ infrastructure such as industrial control systems, financial engines, robotics platforms, HPC pipelines, or scientific instrumentation directly to Spark for large-scale analytics.

### AI/ML and Inference Pipelines

Run high-speed inference using native libraries (e.g., TensorRT, ONNX Runtime, CUDA pipelines) and stream features, embeddings, or predictions into Spark for downstream analytics and training workflows.

### Custom High-Performance Data Connectors

Build specialized connectors for proprietary data stores, binary protocols, in-memory engines, or high-throughput messaging systems that benefit from native execution and fine-grained memory control.

### Interactive Analytics Clients

Develop native dashboards, monitoring tools, and backend services that execute Spark SQL queries through Spark Connect with minimal client-side overhead.

### Edge Gateways and Data Aggregation Services

Use C++ aggregation services or gateway nodes to collect telemetry from distributed environments and forward structured datasets to centralized Spark clusters for processing.

### Performance Testing and Systems Benchmarking

Evaluate Spark Connect performance characteristics such as serialization overhead, network latency, query planning costs, and concurrent client behavior using native workloads.

---

## When Should You Use the C++ Client?

The Spark Connect C++ client is **not a replacement** for Python or Scala Spark APIs. Instead, it enables Spark integration in environments where native execution, performance constraints, or system-level integration are required.

### Use the C++ Client When

* You are integrating Spark into an existing **C++ application or platform**
* Your environment cannot depend on a **JVM runtime**
* You are building **high-performance ingestion or producer services**
* You require integration with:

  * HPC systems
  * scientific computing pipelines
  * robotics or industrial platforms
  * embedded gateways or native backend services
* You run **AI/ML inference pipelines** in C++ and need to forward structured outputs into Spark
* You are implementing proprietary data connectors or binary protocols
* You require precise control over memory layout and data transfer efficiency

### Prefer Python or Scala Spark APIs When

* You are building notebooks or data science workflows
* Your team primarily consists of data engineers or analysts
* You require the full Spark API surface immediately
* Rapid prototyping is more important than native performance
* Your applications already run comfortably in JVM or Python environments

---

## Architecture Deep Dive

The Spark Connect C++ client follows the Spark Connect execution model:

### Client API Layer

Applications use a native C++ API to construct DataFrame operations and Spark SQL queries. These operations are translated into logical execution plans rather than executed locally.

### Logical Plan Construction

The client builds Spark logical plans representing transformations, queries, and actions. No distributed computation occurs inside the client process.

### Serialization Layer

Logical plans and data batches are serialized using:

* Protobuf for query plans and RPC communication
* Apache Arrow for efficient columnar data transfer

### Transport Layer

Communication with the Spark Connect server occurs via:

* gRPC streaming RPCs
* bidirectional execution channels
* Arrow batch streaming

### Spark Server Execution

The Spark Connect server:

* receives logical plans
* executes distributed workloads
* performs query planning and optimization
* returns Arrow-encoded results to the client

This separation enables native applications to leverage Spark’s distributed engine without embedding Spark or JVM runtimes locally.

---

## Getting Started

### 1. Prerequisites

* **Apache Spark 3.5+** with Spark Connect enabled
* **C++17 or later**
* Required libraries:

  * `gRPC`
  * `Protobuf`
  * `Apache Arrow`
  * `uuid`

---

### 2. Build & Run Tests

#### Linux (Ubuntu/Debian)

```bash
# --------------------------------
# Install all required dependencies
# --------------------------------
chmod +x ./install_deps.sh
./install_deps.sh

mkdir build && cd build

# ----------------------------------
# Build the Spark Connect Client
# ----------------------------------
cmake ..
make -j$(nproc)

# --------------------------------
# Make sure Spark is running...
# --------------------------------
docker compose up spark --build

# ---------------------------
# Run Full Test Suite
# ---------------------------
ctest --output-on-failure --verbose
# ctest --verbose --test-arguments=--gtest_color=yes

# -----------------------------
# Run Single Test Suite
# -----------------------------
ctest -R test_dataframe_reader --verbose

# ------------------------------
# Run Single Test Case
# ------------------------------
ctest -R test_dataframe_writer --test-args --gtest_filter=SparkIntegrationTest.ParquetWrite

# --------------------------------
# Run Test Suite directly
# --------------------------------
./test_<suite_name>

# --------------------------------
# Run Single Test Case directly - show output
# --------------------------------
./test_dataframe --gtest_filter=SparkIntegrationTest.DropDuplicates
```

### Mem Checks (Valgrind)

```sh
mkdir -p build && cd build

cmake .. \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCTEST_MEMORYCHECK_COMMAND=/usr/bin/valgrind \
  -DCTEST_MEMORYCHECK_TYPE=Valgrind

valgrind --leak-check=full ./test_dataframe
```

For detailed development setup instructions, see:

* [Setup Guide](https://github.com/irfanghat/spark-connect-cpp/blob/main/docs/SETUP.md)
* [API Reference](https://github.com/irfanghat/spark-connect-cpp/blob/main/docs/API_REFERENCE.md)

---

## License

Apache 2.0