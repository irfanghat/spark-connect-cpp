# Setup Guide

This guide walks you through setting up your development environment for **Spark Connect C++** using **CMake**.


## Prerequisites

Before you begin, ensure the following tools are installed (These can be installed by running the `scripts/install-deps.sh` script on Linux):

### Build Tools
* **CMake** (≥ 3.16 recommended)
* **make** or **ninja**
* **clang/clang++** (C++20 support required)
* **g++** (C++20 support required)
* **pkg-config**

### Core Dependencies
* **Protobuf**
* **gRPC**
* **Apache Arrow**
* **Abseil**

### Runtime Dependencies
* **Docker** & **Docker Compose** (v2 recommended)


## Project Structure
The project uses an **out-of-source CMake build**:

```
spark-connect-cpp/
├── CMakeLists.txt
├── CMakePresets.json
├── CMakeUserPresets.template.json
├── src/
├── tests/
├── build/            
├── datasets/            
├── docker-compose.yaml
├── spark_connect_cpp.pc.in
├── scripts/install-deps.sh
├── .vscode/           # IntelliSense config
└── hooks/
```

---

## Installing Dependencies

### System Dependencies (Linux / Ubuntu)
Run the provided script to install all core libraries:
```bash
chmod +x install_deps.sh
./install_deps.sh
```

## Building the Project (CMake)

### Configure and Build
```bash
cmake --preset local

# --------------------------------------------
# Available configure presets:
# --------------------------------------------
#
#   "local"       - Local Environment
#   "pr_pipeline" - PR Pipeline
# --------------------------------------------

cmake --build --preset local

# --------------------------------------------
# Available configure presets:
# --------------------------------------------
#
#   "local"       - Local Artifacts
#   "pr_pipeline" - PR Pipeline Artifacts
# --------------------------------------------
```

### Run Tests
Ensure an instance of Spark is running locally, e.g. via docker:
```bash
docker compose up spark --build -d
ctest --preset test_local_spark_coverage --verbose --output-on-failure

# -----------------------------------------------------------------------------------------
# Available test presets:
# -----------------------------------------------------------------------------------------
#
#   "test_local_coverage"                  - Spark & Databricks Local Test Coverage
#   "test_local_spark_coverage"            - Spark Local Test Coverage
#   "test_local_databricks_coverage"       - Databricks Local Test Coverage
#   "test_pr_pipeline_coverage"            - Spark & Databricks PR Pipeline Test Coverage
#   "test_pr_pipeline_spark_coverage"      - Spark PR Pipeline Test Coverage
#   "test_pr_pipeline_databricks_coverage" - Databricks PR Pipeline Test Coverage
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Run individual tests
# -----------------------------------------------------------------------------------------
#  ctest --preset test_local_spark_coverage -R DateTimeAndDecimal
#
# Alternatively, you can run the binary directly:
# cd build
# ./spark_connect_cpp_test --gtest_filter=SparkIntegrationTest.DateTimeAndDecimal
# -----------------------------------------------------------------------------------------
```


## VS Code Configuration (`c_cpp_properties.json`)
### Working with the Microsoft C++ extension (Linux)

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

### Working with the Clang extension

### Create/Update the Config
Create a file at `.vscode/settings.json` with the following content:

```json
{
  "clangd.arguments": [
    "--compile-commands-dir=build",
    "--header-insertion=never",
    "--clang-tidy=false"
  ]
}
```

### Why this is necessary
Because `protoc` generates C++ files *during* the build process, they live in `build/gen`. Adding this path to `includePath` allows VS Code to find `base.pb.h`, `commands.pb.h`, etc., which are not part of the static source tree.

#### Generate Coverage:

Ensure `gcovr` and `llvm-18-tools` are installed.
These should be installed if you ran the `scripts/install-deps.sh` script (Linux).

These can also be installed manually by running:

```sh
sudo apt update
sudo apt install -y gcovr llvm-18-tools
```

```sh
cmake --preset local
cmake --build --preset local

# Alternatively, if you're not using CMake presets...
# cmake -S . -B build -DENABLE_COVERAGE=ON
# cmake --build build -j

gcovr -r src \
    --gcov-executable "llvm-cov-18 gcov" \
    --object-directory build \
    --exclude '.*\.pb\.cc' \
    --exclude '.*\.grpc\.pb\.cc' \
    --exclude '.*\.h' \
    --xml-pretty -o coverage.xml \
    --html-details coverage.html \
    --fail-under-line 60 \
    --print-summary \
    --gcov-ignore-errors=no_working_dir_found
```

This will generate a coverage report in **HTML format**.


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
cmake --build --preset local -j2
```