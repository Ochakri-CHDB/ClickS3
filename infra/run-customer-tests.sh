#!/usr/bin/env bash
set -eo pipefail

#############################################################################
# Run all 4 benchmark configurations sequentially
#
# Tests:
#   1. HDD  + Direct 1:1  — current reference topology
#   2. SSD  + Direct 1:1  — SSD upgrade, same topology
#   3. SSD  + HAProxy     — SSD with load balancing
#   4. SSD  + Distributed — distributed MinIO (erasure coded)
#
# Usage:
#   ./infra/run-customer-tests.sh [--duration 5m] [--region eu-west-1]
#
# Each test deploys 6 instances, runs ~10-15 min, then destroys everything.
# Total runtime: ~60-90 min for all 4 tests.
#############################################################################

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DURATION="${1:-5m}"
REGION="${2:-eu-west-1}"

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  ClickS3 — Full Test Suite (4 configurations)                 ║"
echo "║                                                                ║"
echo "║  Test 1: HDD  + Direct 1:1  (reference topology)              ║"
echo "║  Test 2: SSD  + Direct 1:1  (SSD upgrade)                     ║"
echo "║  Test 3: SSD  + HAProxy     (SSD + load balancing)            ║"
echo "║  Test 4: SSD  + Distributed (distributed MinIO, EC:3)         ║"
echo "║                                                                ║"
echo "║  Each test: 3× c5.9xlarge bench + 3× c5.4xlarge MinIO         ║"
echo "║  Duration:  ${DURATION} per test                               ║"
echo "║  Region:    ${REGION}                                          ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

PASSED=0
FAILED=0

run_test() {
  local num="$1" mode="$2" storage="$3" desc="$4"
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  TEST ${num}/4: ${desc}"
  echo "  Mode: ${mode}  |  Storage: ${storage}  |  Duration: ${DURATION}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""

  if "${SCRIPT_DIR}/aws-customer-bench.sh" \
    --mode "$mode" \
    --storage "$storage" \
    --duration "$DURATION" \
    --region "$REGION"; then
    echo "  ✅ TEST ${num} COMPLETE"
    PASSED=$((PASSED + 1))
  else
    echo "  ❌ TEST ${num} FAILED (continuing with next test)"
    FAILED=$((FAILED + 1))
  fi
}

START_TIME=$(date +%s)

run_test 1 "direct"      "hdd" "HDD + Direct 1:1 (reference topology)"
run_test 2 "direct"      "ssd" "SSD + Direct 1:1 (SSD upgrade, same topology)"
run_test 3 "haproxy"     "ssd" "SSD + HAProxy (load balanced)"
run_test 4 "distributed" "ssd" "SSD + Distributed MinIO (EC:3)"

END_TIME=$(date +%s)
ELAPSED=$(( (END_TIME - START_TIME) / 60 ))

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  ALL 4 TESTS COMPLETE                                         ║"
echo "║  Passed: ${PASSED}/4  |  Failed: ${FAILED}/4  |  Total time: ${ELAPSED} min  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "  Results directories:"
ls -d "${SCRIPT_DIR}/../results-bench-"* 2>/dev/null | while read d; do
  echo "    📁 $(basename "$d")"
done
echo ""
