CXX = g++

BASE_CXXFLAGS = -std=c++17 -Wall -O2 -fPIC
BASE_LDFLAGS = -lpthread

########################################
# Fetch Arrow flags
########################################
ARROW_CFLAGS := $(shell pkg-config --cflags arrow parquet arrow-dataset arrow-flight arrow-flight-sql gandiva)
ARROW_LIBS := $(shell pkg-config --libs arrow parquet arrow-dataset arrow-flight arrow-flight-sql gandiva)

########################################
# Fetch gRPC & protobuf flags
########################################
GRPC_CFLAGS := $(shell pkg-config --cflags protobuf grpc++)
GRPC_LIBS := $(shell pkg-config --libs protobuf grpc++)

#########################################
# Fetch UUID flags
#########################################
UUID_CFLAGS := $(shell pkg-config --cflags uuid)
UUID_LIBS := $(shell pkg-config --libs uuid)


CXXFLAGS = $(BASE_CXXFLAGS) $(ARROW_CFLAGS) $(GRPC_CFLAGS) $(UUID_CFLAGS)

########################################################################################################################
#   -labsl_synchronization, is often needed by gRPC components
#
#   We might need other Abseil libraries if linking errors appear,
#   such as -labsl_base, -labsl_core_headers, -labsl_status, etc.
#   currently including synchronization.
#
#   A more comprehensive (and sometimes necessary) list for gRPC could be:
#   LDFLAGS = -lprotobuf -lgrpc++ -lgrpc -lgpr -labsl_synchronization -labsl_base -labsl_core_headers -lpthread -ldl
#######################################################################################################################
LDFLAGS = $(BASE_LDFLAGS) $(GRPC_LIBS) $(ARROW_LIBS) $(UUID_LIBS) -labsl_synchronization



SRC_DIR = src
BUILD_DIR = build
TEST_DIR = tests

PROTO_DIR = $(SRC_DIR)/spark/connect
PROTO_SRCS = $(wildcard $(PROTO_DIR)/*.proto)
PROTO_GEN_DIR = $(BUILD_DIR)/gen

ARROW_DIR = /usr/include/arrow

SUBDIRS := $(shell find $(SRC_DIR) -type d)

SRCS := $(foreach dir,$(SUBDIRS),$(wildcard $(dir)/*.cpp))
OBJS = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(SRCS))

# Filter out main.o from library objects, we may no longer need this
LIB_OBJS = $(filter-out $(BUILD_DIR)/main.o, $(OBJS))

#######################################################################
# This will list all expected generated .cc and .grpc.pb.cc files,
# including their subdirectory path relative to PROTO_GEN_DIR.
#######################################################################


PROTO_GEN_CC_PATHS_REL = $(patsubst $(PROTO_DIR)/%.proto,spark/connect/%.pb.cc,$(PROTO_SRCS)) \
                         $(patsubst $(PROTO_DIR)/%.proto,spark/connect/%.grpc.pb.cc,$(PROTO_SRCS))



#######################################################################
# This will create the full paths to the generated .cc files
#######################################################################


PROTO_GEN_CC_FILES = $(addprefix $(PROTO_GEN_DIR)/, $(PROTO_GEN_CC_PATHS_REL))


#######################################################################
# Derive PROTO_OBJS from PROTO_GEN_CC_PATHS_REL
# This creates object file paths like build/spark/connect/base.pb.o and build/spark/connect/base.grpc.pb.o
#######################################################################

PROTO_OBJS = $(addprefix $(BUILD_DIR)/, $(PROTO_GEN_CC_PATHS_REL:.cc=.o))
# Using .o directly here is fine, as the earlier rules will ensure correct naming.
# Example: spark/connect/base.pb.cc -> build/spark/connect/base.pb.o
# Example: spark/connect/base.grpc.pb.cc -> build/spark/connect/base.grpc.pb.o

INCLUDES = -I$(PROTO_GEN_DIR) -I$(SRC_DIR) -I${ARROW_DIR}

# Library target
LIB_NAME = libspark_client.so
TARGET = $(BUILD_DIR)/$(LIB_NAME)

# Version-specific library names
LIB_VERSION = 1.0.0
LIB_SONAME = $(LIB_NAME).$(word 1,$(subst ., ,$(LIB_VERSION)))
TARGET_VERSIONED = $(TARGET).$(LIB_VERSION)

# Test executables
TEST_DATAFRAME = $(BUILD_DIR)/test_dataframe
TEST_COLUMNS = $(BUILD_DIR)/test_dataframe_columns
ALL_TESTS = $(TEST_DATAFRAME) $(TEST_COLUMNS)

##########################################
# Default target - build library
##########################################

all: $(TARGET)

##########################################
# Compile shared library
##########################################

$(TARGET): $(PROTO_OBJS) $(LIB_OBJS)
	@mkdir -p $(dir $@)
	$(CXX) -shared -Wl,-soname,$(LIB_SONAME) $^ -o $(TARGET_VERSIONED) $(LDFLAGS)
	@ln -sf $(LIB_NAME).$(LIB_VERSION) $(TARGET)
	@ln -sf $(LIB_NAME).$(LIB_VERSION) $(BUILD_DIR)/$(LIB_SONAME)
	@echo "Built shared library: $(TARGET_VERSIONED)"
	@echo "Created symlinks: $(TARGET) -> $(LIB_NAME).$(LIB_VERSION)"

##########################################
# Compile C++ source files
##########################################

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

##########################################
# Compile generated protobuf/grpc files
##########################################

# Generic rule for any generated .cc file in the build/gen/spark/connect/ hierarchy
# This rule will transform build/gen/spark/connect/foo.pb.cc to build/spark/connect/foo.pb.o
# and build/gen/spark/connect/bar.grpc.pb.cc to build/spark/connect/bar.grpc.pb.o
$(BUILD_DIR)/spark/connect/%.o: $(PROTO_GEN_DIR)/spark/connect/%.cc
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

##########################################
# Generate .pb.cc and .grpc.pb.cc files
##########################################

# Output directories reflect the package structure
$(PROTO_GEN_DIR)/spark/connect/%.pb.cc $(PROTO_GEN_DIR)/spark/connect/%.pb.h: $(PROTO_DIR)/%.proto
	@mkdir -p $(dir $@)
	protoc -I=$(SRC_DIR) --experimental_allow_proto3_optional --cpp_out=$(PROTO_GEN_DIR) $<

$(PROTO_GEN_DIR)/spark/connect/%.grpc.pb.cc $(PROTO_GEN_DIR)/spark/connect/%.grpc.pb.h: $(PROTO_DIR)/%.proto
	@mkdir -p $(dir $@)
	protoc -I=$(SRC_DIR) --grpc_out=$(PROTO_GEN_DIR) --experimental_allow_proto3_optional --plugin=protoc-gen-grpc=$(shell which grpc_cpp_plugin) $<


PROTO_GEN_FILES = $(PROTO_GEN_CC_FILES)

##########################################
# Generate all .proto files
##########################################

proto: $(PROTO_GEN_FILES)

$(PROTO_OBJS): $(PROTO_GEN_FILES)

##########################################
# Build Tests
##########################################

# Build DataFrame tests & link against the shared library
$(TEST_DATAFRAME): $(TEST_DIR)/dataframe.cpp $(TARGET)
	@mkdir -p $(dir $@)
	@echo "Building dataframe test..."
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $< \
		-L$(BUILD_DIR) -Wl,-rpath,$(abspath $(BUILD_DIR)) \
		-lspark_client $(LDFLAGS)
	@echo "Built: $@"

# Build DataFrame Columns tests & link against the shared library
$(TEST_COLUMNS): $(TEST_DIR)/dataframe_columns.cpp $(TARGET)
	@mkdir -p $(dir $@)
	@echo "Building dataframe_columns test..."
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $< \
		-L$(BUILD_DIR) -Wl,-rpath,$(abspath $(BUILD_DIR)) \
		-lspark_client $(LDFLAGS)
	@echo "Built: $@"

##########################################
# Test Targets
##########################################

# Build all tests
build-tests: $(ALL_TESTS)
	@echo "========================================="
	@echo "All tests built successfully"
	@echo "========================================="

# Run dataframe test
test-dataframe: $(TEST_DATAFRAME)
	@echo "========================================="
	@echo "Running DataFrame Test"
	@echo "========================================="
	@$(TEST_DATAFRAME)

# Run dataframe_columns test
test-columns: $(TEST_COLUMNS)
	@echo "========================================="
	@echo "Running DataFrame Columns Test"
	@echo "========================================="
	@$(TEST_COLUMNS)

# Run all tests
test: build-tests
	@echo "========================================="
	@echo "Running All Tests"
	@echo "========================================="
	@echo ""
	@echo "--- Running DataFrame Test ---"
	@$(TEST_DATAFRAME) || echo "DataFrame test failed"
	@echo ""
	@echo "--- Running DataFrame Columns Test ---"
	@$(TEST_COLUMNS) || echo "DataFrame Columns test failed"
	@echo ""
	@echo "========================================="
	@echo "Test suite complete"
	@echo "========================================="

##########################################
# Install library
##########################################

install: $(TARGET)
	@echo "Installing library to /usr/local/lib..."
	@sudo cp $(TARGET_VERSIONED) /usr/local/lib/
	@sudo ln -sf $(LIB_NAME).$(LIB_VERSION) /usr/local/lib/$(LIB_NAME)
	@sudo ln -sf $(LIB_NAME).$(LIB_VERSION) /usr/local/lib/$(LIB_SONAME)
	@sudo ldconfig
	@echo "Library installed successfully"
	@echo ""
	@echo "To complete installation, you should also run:"
	@echo "  make install-headers    # Install header files"
	@echo "  make install-pkgconfig  # Install pkg-config file (optional)"

install-headers:
	@echo "Installing headers to /usr/local/include/spark_client..."
	@sudo mkdir -p /usr/local/include/spark_client
	@sudo cp -r $(SRC_DIR)/* /usr/local/include/spark_client/
	@sudo cp -r $(PROTO_GEN_DIR)/* /usr/local/include/spark_client/
	@echo "Headers installed successfully"

install-pkgconfig: spark_client.pc
	@echo "Installing pkg-config file..."
	@sudo mkdir -p /usr/local/lib/pkgconfig
	@sudo cp spark_client.pc /usr/local/lib/pkgconfig/
	@echo "pkg-config file installed successfully"
	@echo "You can now use: pkg-config --cflags --libs spark_client"

spark_client.pc:
	@echo "Creating pkg-config file..."
	@echo "prefix=/usr/local" > $@
	@echo "exec_prefix=\$${prefix}" >> $@
	@echo "libdir=\$${exec_prefix}/lib" >> $@
	@echo "includedir=\$${prefix}/include/spark_client" >> $@
	@echo "" >> $@
	@echo "Name: spark_client" >> $@
	@echo "Description: Spark Connect C++ Client Library" >> $@
	@echo "Version: $(LIB_VERSION)" >> $@
	@echo "Requires: arrow parquet arrow-dataset arrow-flight arrow-flight-sql gandiva protobuf grpc++ uuid" >> $@
	@echo "Libs: -L\$${libdir} -lspark_client -labsl_synchronization" >> $@
	@echo "Cflags: -I\$${includedir}" >> $@
	@echo "pkg-config file created: $@"

install-all: install install-headers install-pkgconfig
	@echo ""
	@echo "========================================="
	@echo "Full installation complete"
	@echo "========================================="
	@echo "You can now compile programs with:"
	@echo "  g++ -std=c++17 \$$(pkg-config --cflags spark_client) your_program.cpp -o your_program \$$(pkg-config --libs spark_client)"
	@echo ""

uninstall:
	@echo "Removing library from /usr/local/lib..."
	@sudo rm -f /usr/local/lib/$(LIB_NAME)*
	@echo "Removing headers from /usr/local/include/spark_client..."
	@sudo rm -rf /usr/local/include/spark_client
	@echo "Removing pkg-config file..."
	@sudo rm -f /usr/local/lib/pkgconfig/spark_client.pc
	@sudo ldconfig
	@echo "Uninstall complete"

##########################################
# Clean build files
##########################################
clean:
	rm -rf $(BUILD_DIR) spark_client.pc

clean-tests:
	rm -f $(ALL_TESTS)

check-deps:
	@echo "Checking for required pkg-config modules..."
	@pkg-config --exists arrow parquet arrow-dataset arrow-flight arrow-flight-sql gandiva && \
	 pkg-config --exists protobuf grpc++ && \
	 echo "spark-connect-cpp::check_deps ==> All dependencies are available." || \
	 (echo "spark-connect-cpp::check_deps ==> Missing required dependencies."; exit 1)

##########################################
# Install Dependencies
##########################################
install-deps:
	@echo "Installing dependencies using install_deps.sh..."
	@./install_deps.sh

help:
	@echo "Spark Connect C++ Client - Makefile targets:"
	@echo ""
	@echo "Building:"
	@echo "  make                  - Build the shared library (default)"
	@echo "  make all              - Same as 'make'"
	@echo "  make proto            - Generate protobuf/gRPC files"
	@echo ""
	@echo "Testing:"
	@echo "  make build-tests      - Build all test executables"
	@echo "  make test             - Build and run all tests"
	@echo "  make test-dataframe   - Build and run dataframe test"
	@echo "  make test-columns     - Build and run columns test"
	@echo ""
	@echo "Installation:"
	@echo "  make install          - Install library to /usr/local/lib"
	@echo "  make install-headers  - Install headers to /usr/local/include"
	@echo "  make install-pkgconfig- Install pkg-config file"
	@echo "  make install-all      - Install everything (library + headers + pkg-config)"
	@echo "  make uninstall        - Remove all installed files"
	@echo ""
	@echo "Cleaning:"
	@echo "  make clean            - Remove all build artifacts"
	@echo "  make clean-tests      - Remove test executables only"
	@echo ""
	@echo "Other:"
	@echo "  make check-deps       - Check if all dependencies are available"
	@echo "  make install-deps     - Install dependencies via script"
	@echo "  make help             - Show this help message"

.PHONY: all clean clean-tests proto install-deps install install-headers install-pkgconfig install-all uninstall \
        check-deps build-tests test test-dataframe test-columns help