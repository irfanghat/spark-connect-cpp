#!/usr/bin/env bash

rm -rf build

cmake --preset pr_pipeline
cmake --build build -j

export LLVM_PROFILE_FILE="coverage-%p.profraw"

ctest --preset test_pr_pipeline_spark_coverage

llvm-profdata merge -sparse coverage-*.profraw -o coverage.profdata

llvm-cov report ./build/tests/spark/spark_connect_cpp_test \
  -instr-profile=coverage.profdata