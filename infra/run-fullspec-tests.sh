#!/usr/bin/env bash
set -eo pipefail

#############################################################################
# Run all 4 full-spec benchmark configurations sequentially
#
# Matching reference hardware:
#   Bench:  r6i.24xlarge × 3 (96 vCPU, 768 GiB)
#   MinIO:  r6i.32xlarge × 3 (128 vCPU, 1024 GiB, 24 drives each)
#
# Tests:
#   1. HDD + Direct 1:1   — current reference topology
#   2. HDD + HAProxy      — HDD with load balancing
#   3. SSD + Direct 1:1   — SSD upgrade, same topology
#   4. SSD + HAProxy      — SSD with load balancing
#
# Each test deploys 6 instances, runs ALL scenarios (compat+insert+merge+
# select+mixed+failures+iops), then auto-destroys.
#
# Usage:
#   ./infra/run-fullspec-tests.sh [--duration 5m] [--region eu-west-1]
#
# Estimated cost: ~$70/hr × 4 tests × ~30min each = ~$140 total
#############################################################################

# Unset proxy vars and ensure AWS profile is exported
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy NO_PROXY no_proxy
: "${AWS_PROFILE:?AWS_PROFILE must be set (e.g. export AWS_PROFILE=your-profile)}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DURATION="5m"
REGION="eu-west-1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration) DURATION="$2"; shift 2 ;;
    --region)   REGION="$2"; shift 2 ;;
    *) echo "Unknown: $1"; exit 1 ;;
  esac
done

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║  ClickS3 — Full-Spec Test Suite (4 configurations)                   ║"
echo "║                                                                        ║"
echo "║  Hardware (matching reference):                                        ║"
echo "║    Bench:  r6i.24xlarge × 3  (96 vCPU, 768 GiB)                      ║"
echo "║    MinIO:  r6i.32xlarge × 3  (128 vCPU, 1024 GiB, 24 drives each)   ║"
echo "║                                                                        ║"
echo "║  Test 1: HDD + Direct 1:1  (reference topology)                      ║"
echo "║  Test 2: HDD + HAProxy     (HDD + load balancing)                    ║"
echo "║  Test 3: SSD + Direct 1:1  (SSD upgrade)                             ║"
echo "║  Test 4: SSD + HAProxy     (SSD + load balancing)                    ║"
echo "║                                                                        ║"
echo "║  Duration:  ${DURATION} per scenario                                   ║"
echo "║  Scenarios: ALL (compat+insert+merge+select+mixed+failures+iops)      ║"
echo "║  Region:    ${REGION}                                                  ║"
echo "║                                                                        ║"
echo "║  Est. cost: ~\$140 total (~\$70/hr × ~2 hrs)                            ║"
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""

PASSED=0
FAILED=0

run_test() {
  local num="$1" mode="$2" storage="$3" desc="$4"
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  TEST ${num}/4: ${desc}"
  echo "  Mode: ${mode}  |  Storage: ${storage}  |  Duration: ${DURATION}"
  echo "  Bench: r6i.24xlarge × 3  |  MinIO: r6i.32xlarge × 3  |  24 drives"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""

  if "${SCRIPT_DIR}/aws-fullspec-bench.sh" \
    --mode "$mode" \
    --storage "$storage" \
    --duration "$DURATION" \
    --region "$REGION"; then
    echo "  TEST ${num} COMPLETE"
    PASSED=$((PASSED + 1))
  else
    echo "  TEST ${num} FAILED — continuing with next test"
    FAILED=$((FAILED + 1))
  fi
}

START_TIME=$(date +%s)

run_test 1 "direct"  "hdd" "HDD + Direct 1:1 — reference topology"
run_test 2 "haproxy" "hdd" "HDD + HAProxy — load balanced"
run_test 3 "direct"  "ssd" "SSD + Direct 1:1 — SSD upgrade, same topology"
run_test 4 "haproxy" "ssd" "SSD + HAProxy — SSD + load balancing"

END_TIME=$(date +%s)
ELAPSED=$(( (END_TIME - START_TIME) / 60 ))

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║  ALL 4 TESTS COMPLETE                                                ║"
echo "║  Passed: ${PASSED}/4  |  Failed: ${FAILED}/4  |  Total time: ${ELAPSED} min      ║"
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""
echo "  Results directories:"
ls -d "${SCRIPT_DIR}/../results-fullspec-"* 2>/dev/null | sort | while read d; do
  echo "    $(basename "$d")"
done
echo ""
