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