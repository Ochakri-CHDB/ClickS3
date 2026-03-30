#!/usr/bin/env bash
set -eo pipefail

#############################################################################
# Run 12 benchmark configs: 2 resource ratios × 2 storage × 3 modes
#
# Ratio A: Equal resources  — bench c5.4xlarge / MinIO c5.4xlarge  (16:16 vCPU)
# Ratio B: 2× MinIO         — bench c5.4xlarge / MinIO c5.9xlarge  (16:36 vCPU)
#
# Storage: SSD (EBS gp3) and HDD (EBS st1)
# Modes:   direct (1:1), haproxy (LB), distributed (EC:3)
#
# Usage:
#   ./infra/run-ratio-tests.sh [--duration 5m] [--region eu-west-1]
#############################################################################

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
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║  ClickS3 — Ratio Benchmark Suite (12 configurations)             ║"
echo "╠══════════════════════════════════════════════════════════════════════╣"
echo "║                                                                    ║"
echo "║  Ratio A (1:1):  bench c5.4xlarge (16 vCPU) = MinIO c5.4xlarge    ║"
echo "║  Ratio B (1:2):  bench c5.4xlarge (16 vCPU) < MinIO c5.9xlarge   ║"
echo "║                                                                    ║"
echo "║  Storage:  SSD (gp3) + HDD (st1)                                  ║"
echo "║  Modes:    Direct 1:1, HAProxy, Distributed EC:3                   ║"
echo "║  Duration: ${DURATION} per test                                    ║"
echo "║  Region:   ${REGION}                                               ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

PASSED=0
FAILED=0
TOTAL=0

run_test() {
  local num="$1" bench_type="$2" minio_type="$3" mode="$4" storage="$5" desc="$6"
  TOTAL=$((TOTAL + 1))
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  TEST ${num}/12: ${desc}"
  echo "  Bench: ${bench_type}  |  MinIO: ${minio_type}"
  echo "  Mode: ${mode}  |  Storage: ${storage}  |  Duration: ${DURATION}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""

  if "${SCRIPT_DIR}/aws-customer-bench.sh" \
    --bench-type "$bench_type" \
    --minio-type "$minio_type" \
    --mode "$mode" \
    --storage "$storage" \
    --duration "$DURATION" \
    --region "$REGION"; then
    echo "  ✅ TEST ${num} COMPLETE"
    PASSED=$((PASSED + 1))
  else
    echo "  ❌ TEST ${num} FAILED (continuing)"
    FAILED=$((FAILED + 1))
  fi
}

START_TIME=$(date +%s)

# ─── Ratio A: Equal resources (1:1) — c5.4xlarge / c5.4xlarge ───────────
echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo "  RATIO A: Equal resources — bench c5.4xlarge (16 vCPU) = MinIO c5.4xlarge (16 vCPU)"
echo "══════════════════════════════════════════════════════════════════════"

run_test  1 "c5.4xlarge" "c5.4xlarge" "direct"      "ssd" "1:1 SSD Direct"
run_test  2 "c5.4xlarge" "c5.4xlarge" "haproxy"     "ssd" "1:1 SSD HAProxy"
run_test  3 "c5.4xlarge" "c5.4xlarge" "distributed" "ssd" "1:1 SSD Distributed EC:3"
run_test  4 "c5.4xlarge" "c5.4xlarge" "direct"      "hdd" "1:1 HDD Direct"
run_test  5 "c5.4xlarge" "c5.4xlarge" "haproxy"     "hdd" "1:1 HDD HAProxy"
run_test  6 "c5.4xlarge" "c5.4xlarge" "distributed" "hdd" "1:1 HDD Distributed EC:3"

# ─── Ratio B: 2× MinIO (1:2) — c5.4xlarge / c5.9xlarge ─────────────────
echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo "  RATIO B: 2× MinIO — bench c5.4xlarge (16 vCPU) < MinIO c5.9xlarge (36 vCPU)"
echo "══════════════════════════════════════════════════════════════════════"

run_test  7 "c5.4xlarge" "c5.9xlarge" "direct"      "ssd" "1:2 SSD Direct"
run_test  8 "c5.4xlarge" "c5.9xlarge" "haproxy"     "ssd" "1:2 SSD HAProxy"
run_test  9 "c5.4xlarge" "c5.9xlarge" "distributed" "ssd" "1:2 SSD Distributed EC:3"
run_test 10 "c5.4xlarge" "c5.9xlarge" "direct"      "hdd" "1:2 HDD Direct"
run_test 11 "c5.4xlarge" "c5.9xlarge" "haproxy"     "hdd" "1:2 HDD HAProxy"
run_test 12 "c5.4xlarge" "c5.9xlarge" "distributed" "hdd" "1:2 HDD Distributed EC:3"

END_TIME=$(date +%s)
ELAPSED=$(( (END_TIME - START_TIME) / 60 ))

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║  ALL 12 TESTS COMPLETE                                            ║"
echo "║  Passed: ${PASSED}/12  |  Failed: ${FAILED}/12  |  Time: ${ELAPSED} min   ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "  Results:"
ls -d "${SCRIPT_DIR}/../results-bench-"* 2>/dev/null | sort | while read d; do
  echo "    $(basename "$d")"
done
echo ""
