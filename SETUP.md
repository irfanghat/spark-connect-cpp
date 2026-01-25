# Setup Guide for the Spark Connect C++ Client

This guide walks you through setting up a development environment for the **Spark Connect C++ Client** using **CMake**.

---

## Prerequisites

Before you begin, ensure the following tools are installed:

### Build Tools

* **CMake** (≥ 3.16 recommended)
* **g++** (C++17 support required)
* **pkg-config**
* **make** (used by CMake generators)

### Core Dependencies

* **Protobuf**
* **gRPC**
* **Apache Arrow**
* **Abseil**

### Runtime Dependencies

* **Docker**
* **Docker Compose** (v2 recommended)

> Tests and some runtime functionality require **Spark to be running via Docker Compose**.

---

## Repository Structure

The project uses an **out-of-source CMake build**:

```
spark-connect-cpp/
├── CMakeLists.txt
├── src/
├── tests/
├── build/              # generated (not committed)
├── docker-compose.yaml
├── install_deps.sh
├── hooks/
└── .vscode/
```

---

## Installing Dependencies

### System Dependencies (Linux / Ubuntu)

You can install all required system dependencies using the provided script:

```bash
chmod +x install_deps.sh
./install_deps.sh
```

This installs:

* Protobuf
* gRPC
* Apache Arrow
* Abseil
* Other required system libraries

> The script is the **recommended** way to set up dependencies.

---

### Manual Installation (Optional)

If you prefer manual installation:

#### Apache Arrow

```bash
sudo apt-get install libarrow-dev libparquet-dev
```

#### Protobuf & gRPC

```bash
sudo apt-get install libprotobuf-dev protobuf-compiler libgrpc++-dev
```

For other platforms, see:

* [https://arrow.apache.org/install/](https://arrow.apache.org/install/)
* [https://grpc.io/docs/languages/cpp/](https://grpc.io/docs/languages/cpp/)

---

## Building the Project (CMake)

### 1. Clone the Repository

```bash
git clone https://github.com/irfanghat/spark-connect-cpp.git
cd spark-connect-cpp
```

---

### 2. Configure the Build

Create a build directory and configure the project:

```bash
mkdir build
cd build
cmake ..
```

This step:

* Detects system dependencies
* Generates build files
* Configures targets and tests

---

### 3. Build

Compile the project:

```bash
make -j$(nproc)
```

or (generator-agnostic):

```bash
cmake --build . -j$(nproc)
```

Build artifacts (libraries, test binaries, generated code) will be placed in `build/`.

---

### 4. Run Tests

Before running tests, **ensure Spark is running**:

```bash
docker compose up spark --build
```

Then run:

```bash
ctest --output-on-failure
```

---

## Running Spark (Required for Tests)

Many tests require a running Spark backend.

From the project root:

```bash
docker compose up spark --build
```

This:

* Builds the Spark container
* Starts Spark Connect
* Exposes required ports for tests

Leave this running while executing tests.

---

## Cleaning the Build

To clean build artifacts:

```bash
rm -rf build
```

(CMake encourages deleting the build directory instead of in-place cleaning.)

---

## Git Hooks (Recommended)

The repository includes **pre-commit** and **pre-push** hooks.

Install them with:

```bash
./install_hooks.sh
```

### Hook Behavior

* **pre-commit**: Ensures the CMake build succeeds
* **pre-push**: Builds + runs tests (requires Docker / Spark)

To bypass (not recommended):

```bash
git commit --no-verify
git push --no-verify
```

---

## VS Code Configuration

VS Code settings are provided in `.vscode/c_cpp_properties.json` to enable:

* IntelliSense
* Go-to-definition
* Error diagnostics

Here's a sample configuration:

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

### Recommended Setup

1. Install the **C/C++** extension:
   [https://marketplace.visualstudio.com/items?itemName=ms-vscode.cpptools](https://marketplace.visualstudio.com/items?itemName=ms-vscode.cpptools)
2. Open the repository root in VS Code
3. Build the project once (`cmake .. && make`) to generate headers
4. IntelliSense will pick up:

   * `build/gen`
   * Generated Protobuf / gRPC files
   * System headers

> VS Code configuration is **for development only**.
> Actual compilation is controlled by **CMake**.

---

## Troubleshooting

### Build Fails

* Ensure dependencies are installed
* Re-run:

  ```bash
  rm -rf build && mkdir build && cd build && cmake ..
  ```

### Tests Fail

* Verify Spark is running:

  ```bash
  docker compose up spark --build
  ```
* Check container logs if needed

### Missing Dependencies

```bash
pkg-config --cflags --libs arrow protobuf grpc++
```

---

## Contributing

Contributions are welcome!
Fork the repo, create a branch, and open a pull request.

---

## License

Licensed under the **Apache License 2.0**

**Repository:**
[https://github.com/irfanghat/spark-connect-cpp](https://github.com/irfanghat/spark-connect-cpp)