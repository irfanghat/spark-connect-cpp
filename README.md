# Spark Connect C++ 

## Overview

This repository hosts a **native C++ client** for **Apache Spark Connect**.

Spark Connect introduces a **decoupled spark-server architecture** for Apache Spark, enabling remote execution of Spark operations. This offers a **high-performance, idiomatic C++ interface** to Spark SQL, with efficient **Apache Arrow-based columnar serialization**.

* Status: _WIP_

![Architecture Diagram](https://github.com/irfanghat/spark-connect-cpp/blob/main/docs/ARICHITECTURE_DIAGRAM.png)

---

## Use Cases

The Spark Connect C++ client is designed for scenarios where performance, low latency, and native integration are critical. Common use cases include:

1. **High-Performance Data Ingestion & Streaming**  
   Ideal for ingesting high-throughput event streams or telemetry data where low-latency processing is required.

2. **Edge and Embedded Analytics**  
   Suitable for analytics on resource-constrained devices or edge gateways, enabling local processing and aggregation before pushing data to Spark.

3. **AI/ML Integration**  
   Can be used to run high-speed inference with native C++ ML libraries (e.g., TensorRT, ONNX Runtime) and stream features or results to Spark for large-scale training or aggregation.

4. **Integration with Legacy or Critical Systems**  
   Bridges enterprise systems, industrial controllers, or scientific instrumentation directly into Spark without requiring JVM dependencies.

5. **Custom High-Performance Connectors**  
   Facilitates the development of specialized connectors for proprietary data stores, in-memory engines, or high-throughput queues.

6. **Low-Latency Interactive Queries**  
   Supports interactive Spark SQL queries in latency-sensitive applications such as dashboards, monitoring tools, and real-time analytics.

7. **Benchmarking and Performance Tooling**  
   Useful for testing Spark Connect performance, measuring end-to-end latency, and evaluating serialization and network overhead.

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
# Run Test Suite
# ---------------------------
ctest --output-on-failure
````

For more details on setting up the project on Unix/Linux, and VSCode, see: [Setup Guide](https://github.com/irfanghat/spark-connect-cpp/blob/main/docs/SETUP.md)

Refer to the following document for API documentation: [API Reference](https://github.com/irfanghat/spark-connect-cpp/blob/main/docs/API_REFERENCE.md)

---

## License

Apache 2.0