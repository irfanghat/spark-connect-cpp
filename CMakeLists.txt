cmake_minimum_required(VERSION 3.15)
project(spark_client VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

#########################################################################

# -----------------------------------------------------------------------
# DEPENDENCIES
# -----------------------------------------------------------------------
find_package(PkgConfig REQUIRED)

# -----------------------------------------------------------------------
# Arrow & Parquet
# -----------------------------------------------------------------------
pkg_check_modules(ARROW REQUIRED IMPORTED_TARGET 
    arrow parquet arrow-dataset arrow-flight arrow-flight-sql gandiva)

# -----------------------------------------------------------------------
# gRPC, Protobuf & Threads
# -----------------------------------------------------------------------
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(Threads REQUIRED)

# -----------------------------------------------------------------------
# UUID & Abseil
# -----------------------------------------------------------------------
pkg_check_modules(UUID REQUIRED IMPORTED_TARGET uuid)
pkg_check_modules(ABSL REQUIRED IMPORTED_TARGET absl_synchronization)

#########################################################################

# -----------------------------------------------------------------------
# Protobuf/gRPC Generation
# -----------------------------------------------------------------------
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
    # Include headers in the list so they are tracked for installation
    list(APPEND PROTO_GENERATED_FILES "${gen_pb_cc}" "${gen_pb_h}" "${gen_grpc_cc}" "${gen_grpc_h}")
endforeach()

add_custom_target(proto DEPENDS ${PROTO_GENERATED_FILES})

#########################################################################

# -----------------------------------------------------------------------
# Library Target
# -----------------------------------------------------------------------
file(GLOB_RECURSE LIB_SOURCES "src/*.cpp")
list(FILTER LIB_SOURCES EXCLUDE REGEX "src/main.cpp")

add_library(spark_client SHARED ${LIB_SOURCES} ${PROTO_GENERATED_FILES})

target_include_directories(spark_client PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    $<BUILD_INTERFACE:${GEN_DIR}>
    $<INSTALL_INTERFACE:include/spark_client>
)

target_link_libraries(spark_client PUBLIC 
    PkgConfig::ARROW 
    PkgConfig::UUID 
    PkgConfig::ABSL
    gRPC::grpc++
    protobuf::libprotobuf
    Threads::Threads
)

set_target_properties(spark_client PROPERTIES 
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

#########################################################################

# -----------------------------------------------------------------------
# Tests (Required for 'ctest' and Docker 'make test')
# -----------------------------------------------------------------------
enable_testing()

# ----------------------------------------------------------------------
# DataFrame Test
# ----------------------------------------------------------------------
add_executable(test_dataframe tests/dataframe.cpp)
target_link_libraries(test_dataframe PRIVATE spark_client)
add_test(NAME DataFrameTest COMMAND test_dataframe)

# ----------------------------------------------------------------------
# Columns Test
# ----------------------------------------------------------------------
add_executable(test_dataframe_columns tests/dataframe_columns.cpp)
target_link_libraries(test_dataframe_columns PRIVATE spark_client)
add_test(NAME ColumnsTest COMMAND test_dataframe_columns)

#########################################################################

# -----------------------------------------------------------------------
# Installation
# -----------------------------------------------------------------------
include(GNUInstallDirs)

install(TARGETS spark_client
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Install original source headers and generated headers
install(DIRECTORY src/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/spark_client)
install(DIRECTORY ${GEN_DIR}/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/spark_client)

# Pkg-config generation
configure_file(spark_client.pc.in spark_client.pc @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/spark_client.pc DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)