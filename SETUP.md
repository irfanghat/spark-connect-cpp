## Build Instructions

### Prerequisites

Before building the project, ensure the following dependencies are installed on your system:

* `protobuf-compiler`
* `libprotobuf-dev`
* `protobuf-compiler-grpc`
* `libgrpc-dev`
* `libgrpc++-dev`
* `build-essential` (for C++ compilation)

You can install them on Ubuntu-based systems using:

```bash
sudo apt update
sudo apt install -y build-essential protobuf-compiler libprotobuf-dev \
                    libgrpc-dev libgrpc++-dev protobuf-compiler-grpc
```

### Generating Protocol Buffers

To generate the required C++ source files from `.proto` definitions, run:

```bash
make proto
```

This will generate `.pb.cc` and `.grpc.pb.cc` files in the `build/gen/` directory based on the `.proto` files in `src/spark/connect/`.

### Building the Project

To compile the source code and link the generated files:

```bash
make
```

This will build the final binary in the `build/` directory. The entry point is defined in `main.cpp`.

### Cleaning Build Artifacts

To remove all generated and compiled files:

```bash
make clean
```

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
                "/usr/local/include"
            ],
            "defines": [],
            "compilerPath": "/usr/bin/g++",
            "cStandard": "c11",
            "cppStandard": "c++17",
            "intelliSenseMode": "linux-gcc-x64"
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