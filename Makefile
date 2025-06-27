CXX = g++
CXXFLAGS = -std=c++17 -Wall -O2
LDFLAGS = -lprotobuf -lpthread -lgrpc++ -lgrpc

SRC_DIR = src
BUILD_DIR = build
PROTO_DIR = $(SRC_DIR)/spark/connect
PROTO_SRCS = $(wildcard $(PROTO_DIR)/*.proto)
PROTO_GEN_DIR = $(BUILD_DIR)/gen

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(SRCS))
PROTO_OBJS = $(patsubst $(PROTO_GEN_DIR)/%.cc,$(BUILD_DIR)/%.pb.o,$(wildcard $(PROTO_GEN_DIR)/*.cc))
INCLUDES = -I$(PROTO_GEN_DIR) -I$(SRC_DIR)

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


$(BUILD_DIR)/%.pb.o: $(PROTO_GEN_DIR)/%.cc
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@


##########################################
# Generate .pb.cc and .grpc.pb.cc files
##########################################


PROTO_GEN_FILES = $(PROTO_SRCS:$(PROTO_DIR)/%.proto=$(PROTO_GEN_DIR)/%.pb.cc) \
                  $(PROTO_SRCS:$(PROTO_DIR)/%.proto=$(PROTO_GEN_DIR)/%.grpc.pb.cc)

$(PROTO_GEN_DIR)/%.pb.cc $(PROTO_GEN_DIR)/%.pb.h: $(PROTO_DIR)/%.proto
	@mkdir -p $(PROTO_GEN_DIR)
	protoc -I=$(SRC_DIR) --cpp_out=$(PROTO_GEN_DIR) $<

$(PROTO_GEN_DIR)/%.grpc.pb.cc $(PROTO_GEN_DIR)/%.grpc.pb.h: $(PROTO_DIR)/%.proto
	@mkdir -p $(PROTO_GEN_DIR)
	protoc -I=$(SRC_DIR) --grpc_out=$(PROTO_GEN_DIR) --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` $<

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

.PHONY: all clean proto
