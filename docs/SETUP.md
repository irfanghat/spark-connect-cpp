# Setup Guide

This guide walks you through setting up your development environment for **Spark Connect C++** using **CMake**.

---

## Prerequisites

Before you begin, ensure the following tools are installed:

### Build Tools
* **CMake** (≥ 3.16 recommended)
* **g++** (C++17 support required)
* **pkg-config**
* **make** or **ninja** (used by CMake generators)

### Core Dependencies
* **Protobuf**
* **gRPC**
* **Apache Arrow**
* **Abseil**

### Runtime Dependencies
* **Docker** & **Docker Compose** (v2 recommended)

---

## Repository Structure
The project uses an **out-of-source CMake build**:
```
spark-connect-cpp/
├── CMakeLists.txt
├── src/
├── tests/
├── build/             # Generated (Not committed)
├── docker-compose.yaml
├── install-deps.sh
├── .vscode/           # Contains IntelliSense config
└── hooks/
```

---

## Installing Dependencies

### System Dependencies (Linux / Ubuntu)
Run the provided script to install all core libraries:
```bash
chmod +x install-deps.sh
./install-deps.sh
```

### Manual Installation (Optional)
```bash
sudo apt-get install libarrow-dev libparquet-dev libprotobuf-dev protobuf-compiler libgrpc++-dev
```

---

## Building the Project (CMake)

### 1. Configure and Build
```bash
mkdir build && cd build
cmake ..
cmake --build . -j$(nproc)
```

### 2. Run Tests
Ensure Spark is running first:
```bash
docker compose up spark --build -d
ctest --output-on-failure
```

---

## VS Code Configuration (`c_cpp_properties.json`)

To enable **IntelliSense**, **Go-to-definition**, and **Error diagnostics**, you must ensure your `.vscode/c_cpp_properties.json` is correctly configured to point to the **generated** files in the build directory.

### Create/Update the Config
Create a file at `.vscode/c_cpp_properties.json` with the following content:

```json
{
    "configurations": [
        {
            "name": "Linux",
            "includePath": [
                "${workspaceFolder}/**",
                "${workspaceFolder}/build/gen",
                "${workspaceFolder}/build/gen/spark",
                "/usr/include",
                "/usr/local/include",
                "/usr/include/arrow",
                "/usr/include/x86_64-linux-gnu"
            ],
            "defines": [],
            "compilerPath": "/usr/bin/g++",
            "cStandard": "c11",
            "cppStandard": "c++17",
            "intelliSenseMode": "linux-gcc-x64",
            "browse": {
                "path": [
                    "${workspaceFolder}/**",
                    "${workspaceFolder}/build/gen",
                    "${workspaceFolder}/build/gen/spark",
                    "/usr/include",
                    "/usr/local/include",
                    "/usr/include/arrow",
                    "/usr/include/x86_64-linux-gnu"
                ],
                "limitSymbolsToIncludedHeaders": true
            }
        }
    ],
    "version": 4
}
```

### Why this is necessary
Because `protoc` generates C++ files *during* the build process, they live in `build/gen`. Adding this path to `includePath` allows VS Code to find `base.pb.h`, `commands.pb.h`, etc., which are not part of the static source tree.

---

## Troubleshooting

### OpenSSL / CURL Symbol Mismatch
If you see `undefined reference to curl_global_init@CURL_OPENSSL_4`, CMake is likely picking up **Conda** libraries instead of system ones.

**Fix:** Force CMake to ignore Conda:
```bash
rm -rf build
cmake -S . -B build \
  -DCMAKE_IGNORE_PATH="/opt/conda;/opt/conda/lib" \
  -DCMAKE_PREFIX_PATH="/usr"
```

### Compiler Terminated Signal (OOM)
If the build fails with `terminated signal terminated program cc1plus`, the system ran out of RAM.

**Fix:** Use fewer parallel jobs:
```bash
cmake --build build -j2
```

---

## License
Licensed under the **Apache License 2.0**

**Repository:** [https://github.com/irfanghat/spark-connect-cpp](https://github.com/irfanghat/spark-connect-cpp)