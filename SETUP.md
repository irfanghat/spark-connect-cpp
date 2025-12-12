# Setup Guide for Spark Connect C++ Client

This guide will walk you through setting up your development environment.

## Prerequisites

Before you begin, make sure you have the following tools and libraries installed:

- **g++**
- **pkg-config** (for checking library dependencies)
- **Protobuf** (for generating Protobuf files)
- **gRPC** (for remote procedure calls)
- **Apache Arrow** (for data transfer and serialization)

Additionally, make sure that you have **make** installed on your system to run the build process.

## Dependencies

### System Dependencies

The project relies on several external libraries which can be installed via `pkg-config`. These libraries are:

- **Apache Arrow**: Provides efficient in-memory data storage.
- **gRPC**: A high-performance RPC framework.
- **Protobuf**: Serialization library for structured data.
- **Abseil**: A collection of C++ libraries used by gRPC.

### Install Required Packages

To check if the required packages are available, you can run:

```bash
make check-deps
````

If any dependencies are missing, you can install them using the following script:

```bash
make install-deps
```

This will automatically install all necessary system dependencies.

### Install Apache Arrow

The C++ client requires **Apache Arrow** to efficiently process and transfer data. Ensure that you have Arrow installed. On Debian/Ubuntu, you can install it using:

```bash
sudo apt-get install libarrow-dev libparquet-dev
```

For other operating systems, follow the Arrow installation instructions from the official [Apache Arrow website](https://arrow.apache.org/install/).

### Install Protobuf and gRPC

Install **Protobuf** and **gRPC** using your system package manager or from the official sources:

#### On Debian/Ubuntu:

```bash
sudo apt-get install libprotobuf-dev protobuf-compiler libgrpc++-dev
```

#### From Source:

If you prefer to install from source, follow these guides:

* [Install Protobuf](https://developers.google.com/protocol-buffers/docs/cpptutorial)
* [Install gRPC](https://grpc.io/docs/languages/cpp/quickstart/)

---

## Building the Project

### 1. Clone the Repository

Clone this repository to your local machine:

```bash
git clone https://github.com/irfanghat/spark-connect-cpp.git
cd spark-connect-cpp
```

### 2. Install Dependencies

You can install the necessary system dependencies using:

```bash
chmod +x install_deps.sh
make install-deps
```

This will run the `install_deps.sh` script, which installs the required libraries for building the project.

### 3. Compile the Project

The project uses a `Makefile` to manage the build process. To compile the project, run:

```bash
make
```

This will compile the C++ source files, generate the necessary Protobuf and gRPC files, and produce the final executable.

### 4. Run the Client

Once the build process is complete, you can run the compiled client example:

```bash
make run
```

This will execute the client using the generated binary. Make sure you have the Spark server running and accessible at the specified address.

---

## Cleaning Up

To remove the compiled files and clean the build directory, you can use:

```bash
make clean
```

---

## Troubleshooting

### Missing Dependencies

If you encounter issues with missing dependencies, ensure that you have the necessary libraries installed. You can also use the `pkg-config` tool to verify the installed libraries:

```bash
pkg-config --cflags --libs arrow protobuf grpc++
```

If any dependencies are missing, the `make check-deps` command will alert you.

---


## VS Code Configuration

If you're using Visual Studio Code (VS Code) as your development environment, a workspace-specific configuration is provided to enable full IntelliSense support, including code completion, error checking, and navigation.

The settings below are located in `.vscode/c_cpp_properties.json`:

```json
{
    "configurations": [
        {
            "name": "Linux",
            "includePath": [
                "${workspaceFolder}/src",
                "${workspaceFolder}/build/gen",
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
                    "${workspaceFolder}/src",
                    "${workspaceFolder}/build/gen",
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

### Explanation

* **includePath**: Lists all the directories where header files may be found. This includes:

  * Your project’s source code (`src`)
  * Generated protobuf/grpc files (`build/gen`)
  * Standard system include paths (`/usr/include`, `/usr/local/include`)
* **compilerPath**: Specifies the path to the C++ compiler used by IntelliSense (`/usr/bin/g++`).
* **cStandard / cppStandard**: Defines the C and C++ standards used (`C11` and `C++17` respectively).
* **intelliSenseMode**: Configures the IntelliSense engine to match the compiler and system (`linux-gcc-x64`).

> ⚠️ **Note:** This configuration is used by VS Code for development assistance only. Actual compilation is handled by the `Makefile`.

### Setting Up

1. Make sure the [C/C++ extension for VS Code](https://marketplace.visualstudio.com/items?itemName=ms-vscode.cpptools) is installed.
2. Open the project folder in VS Code.
3. IntelliSense should automatically pick up this configuration.
4. Building Apache Arrow from source: https://arrow.apache.org/docs/developers/cpp/building.html#building-arrow-cpp

## Contributing

If you wish to contribute to the project, feel free to fork the repository and create a pull request. For any issues, open an issue on the GitHub repository.

---

## License

This project is licensed under the **Apache License 2.0**.

**Repository:** [https://github.com/irfanghat/spark-connect-cpp](https://github.com/irfanghat/spark-connect-cpp)