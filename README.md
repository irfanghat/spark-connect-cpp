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

### 2. Build & Run Tests

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
ctest --output-on-failure
```

For more details on setting up the projects on Unix/Linux, and VsCode, see: [Setup Guide](https://github.com/spark-connect-cpp/docs/SETUP.md)

---

## License

Apache 2.0
