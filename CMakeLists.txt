cmake_minimum_required(VERSION 3.15)
project(spark_connect_cpp VERSION 0.0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)


#########################################################################
# DEPENDENCIES
#########################################################################
find_package(PkgConfig REQUIRED)

# ---------------------------------
# Arrow & Parquet
# ---------------------------------
pkg_check_modules(ARROW REQUIRED IMPORTED_TARGET 
    arrow parquet arrow-dataset arrow-flight arrow-flight-sql gandiva)

# ---------------------------------
# gRPC, Protobuf & Threads
# ---------------------------------
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(Threads REQUIRED)

# ---------------------------------
# Google Test
# ---------------------------------
find_package(GTest REQUIRED)

# ---------------------------------
# UUID & Abseil
# ---------------------------------
pkg_check_modules(UUID REQUIRED IMPORTED_TARGET uuid)
pkg_check_modules(ABSL REQUIRED IMPORTED_TARGET absl_synchronization)

# ---------------------------------
# Protobuf/gRPC Generation
# ---------------------------------
find_program(PROTOC_EXECUTABLE NAMES protoc)
find_program(GRPC_CPP_PLUGIN NAMES grpc_cpp_plugin)

set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/spark/connect")
set(GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/gen")
file(MAKE_DIRECTORY ${GEN_DIR})

file(GLOB PROTO_SRCS "${PROTO_DIR}/*.proto")
set(PROTO_GENERATED_FILES)

foreach(proto_file ${PROTO_SRCS})
    get_filename_component(proto_name ${proto_file} NAME_WE)
    
    set(gen_pb_cc "${GEN_DIR}/spark/connect/${proto_name}.pb.cc")
    set(gen_pb_h  "${GEN_DIR}/spark/connect/${proto_name}.pb.h")
    set(gen_grpc_cc "${GEN_DIR}/spark/connect/${proto_name}.grpc.pb.cc")
    set(gen_grpc_h  "${GEN_DIR}/spark/connect/${proto_name}.grpc.pb.h")
    
    add_custom_command(
        OUTPUT "${gen_pb_cc}" "${gen_pb_h}" "${gen_grpc_cc}" "${gen_grpc_h}"
        COMMAND ${PROTOC_EXECUTABLE}
        ARGS --cpp_out "${GEN_DIR}"
             --grpc_out "${GEN_DIR}"
             --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
             --experimental_allow_proto3_optional
             -I "${CMAKE_CURRENT_SOURCE_DIR}/src"
             "${proto_file}"
        DEPENDS "${proto_file}"
        COMMENT "Running gRPC/Protobuf compiler on ${proto_file}"
        VERBATIM
    )
    list(APPEND PROTO_GENERATED_FILES "${gen_pb_cc}" "${gen_pb_h}" "${gen_grpc_cc}" "${gen_grpc_h}")
endforeach()

add_custom_target(proto DEPENDS ${PROTO_GENERATED_FILES})

#########################################################################
# Library Target
#########################################################################
file(GLOB_RECURSE LIB_SOURCES "src/*.cpp")
list(FILTER LIB_SOURCES EXCLUDE REGEX "src/main.cpp")

add_library(spark_connect_cpp STATIC ${LIB_SOURCES} ${PROTO_GENERATED_FILES})
# add_library(spark_connect_cpp SHARED ${LIB_SOURCES} ${PROTO_GENERATED_FILES})

target_include_directories(spark_connect_cpp PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    $<BUILD_INTERFACE:${GEN_DIR}>
    $<INSTALL_INTERFACE:include/spark_connect_cpp>
)

target_link_libraries(spark_connect_cpp PUBLIC 
    PkgConfig::ARROW 
    PkgConfig::UUID 
    PkgConfig::ABSL
    gRPC::grpc++
    protobuf::libprotobuf
    Threads::Threads
)

set_target_properties(spark_connect_cpp PROPERTIES 
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

#########################################################################
# Testing Configuration
#########################################################################
enable_testing()

# -------------------------------------------------------
# Helper macro to reduce boilerplate for adding gTests
# -------------------------------------------------------
macro(add_spark_test TEST_NAME SOURCE_FILE)
    add_executable(${TEST_NAME} ${SOURCE_FILE})
    target_link_libraries(${TEST_NAME} PRIVATE 
        spark_connect_cpp 
        GTest::gtest
        GTest::gmock 
        GTest::gtest_main
    )
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
    
    # -------------------------------------------------------
    # Automatically tag databricks tests as "dbrx"
    # -------------------------------------------------------
    if(${TEST_NAME} MATCHES "databricks")
        set_tests_properties(${TEST_NAME} PROPERTIES LABELS "dbrx")
    endif()

    set_tests_properties(${TEST_NAME} PROPERTIES ENVIRONMENT "GTEST_COLOR=1")
endmacro()

######################################################
# Test Suite Definitions
######################################################

add_spark_test(test_dataframe tests/dataframe.cpp)
add_spark_test(test_dataframe_columns tests/dataframe_columns.cpp)
add_spark_test(test_dataframe_reader tests/dataframe_reader.cpp)
add_spark_test(test_dataframe_writer tests/dataframe_writer.cpp)

# -----------------------------------------------------------------------
# The following tests will be labeled "dbrx" automatically
# Since these specifically require an active Databricks connection,
# we can conditionally exclude them.
# -----------------------------------------------------------------------
add_spark_test(test_traditional_databricks_cluster tests/traditional_databricks_cluster.cpp)
add_spark_test(test_serverless_databricks_cluster tests/serverless_databricks_cluster.cpp)

#########################################################################
# Installation
#########################################################################
include(GNUInstallDirs)

install(TARGETS spark_connect_cpp
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(DIRECTORY src/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/spark_connect_cpp)
install(DIRECTORY ${GEN_DIR}/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/spark_connect_cpp)

configure_file(spark_connect_cpp.pc.in spark_connect_cpp.pc @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/spark_connect_cpp.pc DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

#########################################################################
# CPack
#########################################################################
set(CPACK_GENERATOR "TGZ")
set(CPACK_PACKAGE_NAME "spark-connect-cpp")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Spark Connect C++ Static Library")
set(CPACK_PACKAGE_VENDOR "Irfan M. Ghat")
set(CPACK_PACKAGE_FILE_NAME "spark-connect-cpp-${PROJECT_VERSION}-linux-amd64")

include(CPack)