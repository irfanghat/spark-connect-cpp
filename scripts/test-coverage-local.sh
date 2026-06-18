#!/usr/bin/env bash

# ----------------------------------------------------------------------------
#
# Reproduces the CI coverage pipeline locally (For debugging purposes):
#   configure -> build -> ctest (with LLVM_PROFILE_FILE) -> llvm-profdata merge -> llvm-cov export/show/report -> threshold check
#
# Usage:
#   ./test-coverage-local.sh                          # Full run
#   ./test-coverage-local.sh --skip-tests             # Reuse existing build + profraw, just regenerate coverage
#   ./test-coverage-local.sh --threshold=70           # Override the line-coverage gate
#   ./test-coverage-local.sh --binary-pattern="*Test" # Override how test binaries are discovered
#
# ----------------------------------------------------------------------------

set -euo pipefail

CONFIGURE_PRESET="pr_pipeline"
TEST_PRESET="test_pr_pipeline_coverage"
BUILD_DIR="build"
THRESHOLD=60
BINARY_PATTERN="*_test"
SKIP_TESTS=0
IGNORE_REGEX='.*\.pb\.cc$|.*\.grpc\.pb\.cc$|.*\.h$'

for arg in "$@"; do
  case "$arg" in
    --skip-tests) SKIP_TESTS=1 ;;
    --threshold=*) THRESHOLD="${arg#*=}" ;;
    --binary-pattern=*) BINARY_PATTERN="${arg#*=}" ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

ROOT_DIR="$(pwd)"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: required tool '$1' not found on PATH." >&2
    exit 1
  fi
}

echo "==> Checking required tools"
require_cmd cmake
require_cmd ninja
require_cmd ctest
require_cmd clang
require_cmd llvm-profdata-18
require_cmd llvm-cov-18
require_cmd pip

if [ "$SKIP_TESTS" -eq 0 ]; then
  echo "==> Configuring (${CONFIGURE_PRESET})"
  cmake --preset "$CONFIGURE_PRESET"

  echo "==> Building"
  cmake --build --preset "$CONFIGURE_PRESET"

  mkdir -p "$BUILD_DIR/coverage"
  rm -f "$BUILD_DIR"/coverage/*.profraw

  echo "==> Running tests (${TEST_PRESET})"
  LLVM_PROFILE_FILE="$ROOT_DIR/$BUILD_DIR/coverage/%p.profraw" \
    ctest --preset "$TEST_PRESET" --test-dir "$BUILD_DIR"
else
  echo "==> Skipping configure/build/test, reusing existing build dir"
fi

shopt -s nullglob
PROFRAW_FILES=("$BUILD_DIR"/coverage/*.profraw)
shopt -u nullglob
if [ ${#PROFRAW_FILES[@]} -eq 0 ]; then
  echo "ERROR: no .profraw files found in $BUILD_DIR/coverage. Tests didn't run, or LLVM_PROFILE_FILE wasn't honored." >&2
  exit 1
fi
echo "==> Found ${#PROFRAW_FILES[@]} profraw file(s)"

echo "==> Merging profile data"
llvm-profdata-18 merge -sparse "$BUILD_DIR"/coverage/*.profraw -o "$BUILD_DIR/coverage/merged.profdata"

echo "==> Discovering test binaries (pattern: ${BINARY_PATTERN})"
mapfile -t BINARY_LIST < <(find "$BUILD_DIR" -maxdepth 3 -type f -executable -name "$BINARY_PATTERN")
if [ ${#BINARY_LIST[@]} -eq 0 ]; then
  echo "ERROR: no executables matched pattern '${BINARY_PATTERN}' under $BUILD_DIR." >&2
  echo "        Re-run with --binary-pattern=<glob> to point at the right binaries." >&2
  exit 1
fi
echo "    Matched: ${BINARY_LIST[*]}"

TEST_BINARIES=()
for bin in "${BINARY_LIST[@]}"; do
  TEST_BINARIES+=("-object=$bin")
done

echo "==> Exporting lcov & converting to Cobertura XML"
llvm-cov-18 export \
  "${TEST_BINARIES[@]}" \
  -instr-profile="$BUILD_DIR/coverage/merged.profdata" \
  -ignore-filename-regex="$IGNORE_REGEX" \
  -format=lcov > "$BUILD_DIR/coverage.info"

if ! pip show lcov_cobertura >/dev/null 2>&1; then
  pip install --quiet --break-system-packages lcov_cobertura
fi
lcov_cobertura "$BUILD_DIR/coverage.info" -o "$BUILD_DIR/coverage.xml"

echo "==> Generating HTML report"
llvm-cov-18 show \
  "${TEST_BINARIES[@]}" \
  -instr-profile="$BUILD_DIR/coverage/merged.profdata" \
  -ignore-filename-regex="$IGNORE_REGEX" \
  -format=html -output-dir="$BUILD_DIR/coverage_html"

echo "==> Generating summary report"
llvm-cov-18 report \
  "${TEST_BINARIES[@]}" \
  -instr-profile="$BUILD_DIR/coverage/merged.profdata" \
  -ignore-filename-regex="$IGNORE_REGEX" \
  | tee "$BUILD_DIR/coverage_summary.txt"

LINE_PCT=$(grep TOTAL "$BUILD_DIR/coverage_summary.txt" | awk '{print $(NF-1)}' | tr -d '%')

echo ""
echo "-----------------------------------------------------------"
echo " Line coverage: ${LINE_PCT}%  (threshold: ${THRESHOLD}%)"
echo " XML report:    $BUILD_DIR/coverage.xml"
echo " HTML report:   $BUILD_DIR/coverage_html/index.html"
echo "-----------------------------------------------------------"

if awk -v pct="$LINE_PCT" -v threshold="$THRESHOLD" 'BEGIN { exit (pct < threshold) }'; then
  echo "RESULT: PASS"
  exit 0
else
  echo "RESULT: FAIL (below ${THRESHOLD}% threshold)"
  exit 1
fi