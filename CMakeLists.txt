cmake_minimum_required(VERSION 4.2.2)


set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


# Declare project

set(PROJECT_NAME "spark_connect_cpp")

project(${PROJECT_NAME})


# Dependencies - loaded from vcpkg CMake toolchain

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)
find_package(unofficial-libuuid CONFIG REQUIRED)
find_package(GTest REQUIRED)


# Spark Connect Proto library

set(SPARK_CONNECT_PROTO_LIB "spark_connect")
set(SPARK_CONNECT_PROTO_BUILD "${CMAKE_CURRENT_BINARY_DIR}/gen")

file(GLOB_RECURSE SPARK_CONNECT_PROTO_SRC CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/*.proto")

add_library(${SPARK_CONNECT_PROTO_LIB} STATIC ${SPARK_CONNECT_PROTO_SRC})

target_link_libraries(${SPARK_CONNECT_PROTO_LIB} PUBLIC
        protobuf::libprotobuf
        gRPC::grpc++
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>"
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>"
)

message("Spark Connect C++ source and header files build in ${SPARK_CONNECT_PROTO_BUILD}")

protobuf_generate(
        TARGET "${SPARK_CONNECT_PROTO_LIB}"
        IMPORT_DIRS "${CMAKE_SOURCE_DIR}/src"
        PROTOC_OUT_DIR "${SPARK_CONNECT_PROTO_BUILD}"
)

protobuf_generate(
        TARGET "${SPARK_CONNECT_PROTO_LIB}"
        LANGUAGE grpc
        GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
        PLUGIN "protoc-gen-grpc=\$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
        IMPORT_DIRS "${CMAKE_SOURCE_DIR}/src"
        PROTOC_OUT_DIR "${SPARK_CONNECT_PROTO_BUILD}"
)

target_include_directories(${SPARK_CONNECT_PROTO_LIB} PUBLIC
        "$<BUILD_INTERFACE:${SPARK_CONNECT_PROTO_BUILD}>"
)


# Project library

message("Project library and link libraries ${PROJECT_NAME}")

file(GLOB_RECURSE PROJECT_SOURCES CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/*.cpp")

list(FILTER PROJECT_SOURCES EXCLUDE REGEX "${CMAKE_SOURCE_DIR}/src/main.cpp")

add_library(${PROJECT_NAME} STATIC ${PROJECT_SOURCES})

target_link_libraries(${PROJECT_NAME} PUBLIC
        ${SPARK_CONNECT_PROTO_LIB}
        unofficial::UUID::uuid
)

target_include_directories(${PROJECT_NAME} PUBLIC
        "$<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>"
)


# Test artifact

message("Project tests with GTest")

file(GLOB_RECURSE TEST_SRC_FILES CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/tests/*.cpp")

add_executable(${PROJECT_NAME}_test ${TEST_SRC_FILES})

target_link_libraries(${PROJECT_NAME}_test PRIVATE
        ${PROJECT_NAME}
        GTest::gtest
        GTest::gmock 
        GTest::gtest_main
)

enable_testing()

gtest_discover_tests(${PROJECT_NAME}_test)
