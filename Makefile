CXX = g++

BASE_CXXFLAGS = -std=c++17 -Wall -O2
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

PROTO_DIR = $(SRC_DIR)/spark/connect
PROTO_SRCS = $(wildcard $(PROTO_DIR)/*.proto)
PROTO_GEN_DIR = $(BUILD_DIR)/gen

ARROW_DIR = /usr/include/arrow

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(SRCS))


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

TARGET = $(BUILD_DIR)/spark_client

##########################################
# Default target
##########################################

all: $(TARGET)

##########################################
# Compile target binary
##########################################

$(TARGET): $(PROTO_OBJS) $(OBJS)
	@mkdir -p $(dir $@)
	$(CXX) $^ -o $@ $(LDFLAGS)

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
	protoc -I=$(SRC_DIR) --cpp_out=$(PROTO_GEN_DIR) $<

$(PROTO_GEN_DIR)/spark/connect/%.grpc.pb.cc $(PROTO_GEN_DIR)/spark/connect/%.grpc.pb.h: $(PROTO_DIR)/%.proto
	@mkdir -p $(dir $@)
	protoc -I=$(SRC_DIR) --grpc_out=$(PROTO_GEN_DIR) --plugin=protoc-gen-grpc=$(shell which grpc_cpp_plugin) $<


PROTO_GEN_FILES = $(PROTO_GEN_CC_FILES)

##########################################
# Generate all .proto files
##########################################

proto: $(PROTO_GEN_FILES)

##########################################
# Run the main executable
##########################################

run: $(TARGET)
	$(TARGET)

##########################################
# Clean build files
##########################################
clean:
	rm -rf $(BUILD_DIR)

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

.PHONY: all clean proto install-deps run check-deps