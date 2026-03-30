#!/usr/bin/env python3
"""
Merge multiple ClickS3 node reports into a combined verdict.

Usage:
    python3 merge-reports.py report-node1.json report-node2.json report-node3.json
"""

import json
import sys
from collections import defaultdict


def load_reports(paths):
    reports = []
    for path in paths:
        try:
            with open(path) as f:
                reports.append(json.load(f))
        except Exception as e:
            print(f"  WARNING: Could not load {path}: {e}", file=sys.stderr)
    return reports


def merge_stats(reports):
    """Aggregate operation stats across all nodes."""
    combined = defaultdict(lambda: {
        "count": 0, "success_count": 0, "error_count": 0,
        "bytes_transferred": 0, "p50_ms": [], "p95_ms": [], "p99_ms": [],
        "throughput_mbps": 0, "ops_per_sec": 0,
    })

    for report in reports:
        for scenario in report.get("scenarios", []):
            for op_type, stats in scenario.get("stats", {}).items():
                c = combined[op_type]
                c["count"] += stats.get("count", 0)
                c["success_count"] += stats.get("success_count", 0)
                c["error_count"] += stats.get("error_count", 0)
                c["bytes_transferred"] += stats.get("bytes_transferred", 0)
                c["throughput_mbps"] += stats.get("throughput_mbps", 0)
                c["ops_per_sec"] += stats.get("ops_per_sec", 0)
                if stats.get("p50_ms", 0) > 0:
                    c["p50_ms"].append(stats["p50_ms"])
                if stats.get("p95_ms", 0) > 0:
                    c["p95_ms"].append(stats["p95_ms"])
                if stats.get("p99_ms", 0) > 0:
                    c["p99_ms"].append(stats["p99_ms"])

    return combined


def print_combined_report(reports):
    total_checks = 0
    passed_checks = 0
    failed_names = []

    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║     ClickS3 — Combined Multi-Node Report                   ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()

    for report in reports:
        node_id = report.get("node_id", "unknown")
        verdict = report.get("verdict", "N/A")
        role = report.get("role", "N/A")
        res = report.get("server_resources", {})
        cpu = res.get("cpu_cores", "?")
        ram = res.get("total_ram_gb", 0)

        icon = "✓" if verdict == "PASS" else "✗"
        print(f"  {icon} {node_id:30s}  {verdict:6s}  role={role}  {cpu} vCPU / {ram:.1f} GB")

        for scenario in report.get("scenarios", []):
            for check in scenario.get("checks", []):
                total_checks += 1
                if check.get("passed"):
                    passed_checks += 1
                else:
                    failed_names.append(f"{node_id}: {check['name']}")

    print()

    # Combined stats
    combined = merge_stats(reports)
    print(f"  {'Operation':20s} {'Total Ops':>10s} {'Errors':>8s} {'Avg P50ms':>10s} {'Avg P99ms':>10s} {'Total MB/s':>10s}")
    print(f"  {'─'*72}")
    for op, c in sorted(combined.items()):
        if c["count"] == 0:
            continue
        avg_p50 = sum(c["p50_ms"]) / len(c["p50_ms"]) if c["p50_ms"] else 0
        avg_p99 = sum(c["p99_ms"]) / len(c["p99_ms"]) if c["p99_ms"] else 0
        print(f"  {op:20s} {c['count']:10d} {c['error_count']:8d} {avg_p50:10.1f} {avg_p99:10.1f} {c['throughput_mbps']:10.1f}")

    # Verdict
    print()
    if total_checks == 0:
        verdict = "WARN"
        summary = "No checks collected"
    elif passed_checks == total_checks:
        verdict = "PASS"
        summary = f"All {total_checks} checks passed across {len(reports)} nodes."
    elif passed_checks / total_checks >= 0.8:
        verdict = "WARN"
        summary = f"{passed_checks}/{total_checks} checks passed ({100*passed_checks//total_checks}%)."
    else:
        verdict = "FAIL"
        summary = f"{passed_checks}/{total_checks} checks passed ({100*passed_checks//total_checks}%)."

    print("╔══════════════════════════════════════════════════════════════╗")
    if verdict == "PASS":
        print("║             COMBINED VERDICT: PASS                         ║")
    elif verdict == "FAIL":
        print("║             COMBINED VERDICT: FAIL                         ║")
    else:
        print("║             COMBINED VERDICT: WARN                         ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  {summary:58s}║")
    print(f"║  Nodes: {len(reports):<50d}║")
    print(f"║  Checks: {passed_checks}/{total_checks} passed{' ':41s}║")
    print("╚══════════════════════════════════════════════════════════════╝")

    if failed_names:
        print()
        print("  Failed checks:")
        for name in failed_names[:20]:
            print(f"    ✗ {name}")
        if len(failed_names) > 20:
            print(f"    ... and {len(failed_names) - 20} more")

    # Write combined JSON
    combined_report = {
        "nodes": len(reports),
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "verdict": verdict,
        "summary": summary,
        "node_reports": reports,
    }
    out_path = sys.argv[1].rsplit("/", 1)[0] + "/combined-report.json"
    with open(out_path, "w") as f:
        json.dump(combined_report, f, indent=2)
    print(f"\n  Combined report: {out_path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: merge-reports.py report1.json [report2.json ...]")
        sys.exit(1)

    reports = load_reports(sys.argv[1:])
    if not reports:
        print("No valid reports found.")
        sys.exit(1)

    print_combined_report(reports)


if __name__ == "__main__":
    main()
