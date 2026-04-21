#######################################################################################################
# CMAKE CONFIGURATION
# Specify CMake version and C++ standard
#######################################################################################################

cmake_minimum_required(VERSION 3.15)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


#######################################################################################################
# PROJECT DECLARATION
# Define project name and version
#######################################################################################################

set(PROJECT_NAME "spark_connect_cpp")

project(${PROJECT_NAME})


#######################################################################################################
# FIND DEPENDENCIES
# Loaded with PkgConfig
#######################################################################################################
message(STATUS "Using PKG_CONFIG_PATH: $ENV{PKG_CONFIG_PATH}")

find_package(PkgConfig REQUIRED)

pkg_check_modules(PROTOBUF REQUIRED IMPORTED_TARGET protobuf)
pkg_check_modules(GRPC REQUIRED IMPORTED_TARGET grpc++)
pkg_check_modules(ARROW REQUIRED IMPORTED_TARGET arrow)
pkg_check_modules(PARQUET REQUIRED IMPORTED_TARGET parquet)
pkg_check_modules(UUID REQUIRED IMPORTED_TARGET uuid)
pkg_check_modules(GTEST REQUIRED IMPORTED_TARGET gtest_main gmock)

find_package(Protobuf REQUIRED)
find_program(GRPC_CPP_PLUGIN NAMES grpc_cpp_plugin)


#######################################################################################################
# SPARK CONNECT PROTO LIB
# Generate proto buffer data and gRPC service source files (*.pb.h, *.grpc.pb.h, *.pb.cc, *.grpc.pb.cc)
#######################################################################################################

set(SPARK_CONNECT_PROTO_LIB "spark_connect")
set(SPARK_CONNECT_PROTO_BUILD "${CMAKE_CURRENT_BINARY_DIR}/gen")

file(GLOB_RECURSE SPARK_CONNECT_PROTO_SRC CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/*.proto")

add_library(${SPARK_CONNECT_PROTO_LIB} STATIC ${SPARK_CONNECT_PROTO_SRC})

target_link_libraries(${SPARK_CONNECT_PROTO_LIB} PUBLIC
        PkgConfig::PROTOBUF
        PkgConfig::GRPC
        PkgConfig::ARROW
        PkgConfig::PARQUET
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
        PLUGIN "protoc-gen-grpc=${GRPC_CPP_PLUGIN}"
        IMPORT_DIRS "${CMAKE_SOURCE_DIR}/src"
        PROTOC_OUT_DIR "${SPARK_CONNECT_PROTO_BUILD}"
)

target_include_directories(${SPARK_CONNECT_PROTO_LIB} PUBLIC
        "$<BUILD_INTERFACE:${SPARK_CONNECT_PROTO_BUILD}>"
)


#######################################################################################################
# PROJECT LIBRARY
#######################################################################################################

message("Project library and link libraries ${PROJECT_NAME}")

file(GLOB_RECURSE PROJECT_SOURCES CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/*.cpp")

list(FILTER PROJECT_SOURCES EXCLUDE REGEX "${CMAKE_SOURCE_DIR}/src/main.cpp")

add_library(${PROJECT_NAME} STATIC ${PROJECT_SOURCES})

target_link_libraries(${PROJECT_NAME} PUBLIC
        ${SPARK_CONNECT_PROTO_LIB}
        PkgConfig::UUID
)

target_include_directories(${PROJECT_NAME} PUBLIC
        "$<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>"
)


#######################################################################################################
# TEST ARTIFACT
# Integration tests using GTest
#######################################################################################################

message("Project tests with GTest")

file(GLOB_RECURSE TEST_SRC_FILES CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/tests/*.cpp")

add_executable(${PROJECT_NAME}_test ${TEST_SRC_FILES})

target_link_libraries(${PROJECT_NAME}_test PRIVATE
        ${PROJECT_NAME}
        PkgConfig::GTEST
)

include(GoogleTest)

enable_testing()

gtest_discover_tests(${PROJECT_NAME}_test)

#######################################################################################################
# PROJECT INSTALLATION
#######################################################################################################

include(GNUInstallDirs)

install(TARGETS ${PROJECT_NAME}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(DIRECTORY src/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME})
install(DIRECTORY ${SPARK_CONNECT_PROTO_BUILD}/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME})

configure_file(${PROJECT_NAME}.pc.in ${PROJECT_NAME}.pc @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}.pc DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)


#######################################################################################################
# PROJECT DISTRIBUTION
#######################################################################################################

set(CPACK_PACKAGE_NAME "spark-connect-cpp")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Spark Connect C++ Static Library")
set(CPACK_PACKAGE_VENDOR "Irfan M. Ghat")
set(CPACK_PACKAGE_FILE_NAME "spark-connect-cpp-${PROJECT_VERSION}-linux-amd64")
set(CPACK_GENERATOR "TGZ")

include(CPack)