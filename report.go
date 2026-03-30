package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Check represents a pass/fail test criterion
type Check struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

// ScenarioResult holds the results of a single scenario run
type ScenarioResult struct {
	Name                string               `json:"name"`
	Duration            time.Duration        `json:"duration"`
	Stats               map[OpType]*OpStats  `json:"stats"`
	Checks              []Check              `json:"checks"`
	StorageCapabilities *StorageCapabilities `json:"storage_capabilities,omitempty"`
	CapacityReport      []CHConfig           `json:"capacity_report,omitempty"`
}

// ValidationResult is a single metric comparison in the validation guide.
type ValidationResult struct {
	Label    string  `json:"label"`
	Expected string  `json:"expected"`
	Measured float64 `json:"measured"`
	HasData  bool    `json:"has_data"`
	Status   string  `json:"status"`
}

// FullReport is the complete benchmark report
type FullReport struct {
	Version         string              `json:"version"`
	Timestamp       time.Time           `json:"timestamp"`
	NodeID          string              `json:"node_id"`
	Endpoint        string              `json:"endpoint"`
	Bucket          string              `json:"bucket"`
	Role            string              `json:"role"`
	Resources       *ServerResources    `json:"server_resources"`
	Scenarios       []*ScenarioResult   `json:"scenarios"`
	Verdict         string              `json:"verdict"`
	Summary         string              `json:"summary"`
	TotalTime       time.Duration       `json:"total_time"`
	ValidationGuide []ValidationResult  `json:"validation_guide,omitempty"`
	SummaryBlock    string              `json:"summary_block,omitempty"`
}

// checkLatencyPass creates a Check comparing actual latency against a PASS threshold.
func checkLatencyPass(name string, actual, passThreshold float64) Check {
	return Check{
		Name:   name,
		Passed: actual <= passThreshold,
		Detail: fmt.Sprintf("%.1fms (pass: ≤%.0fms)", actual, passThreshold),
	}
}

// PrintScenarioHeader prints a scenario header
func PrintScenarioHeader(name, description string) {
	fmt.Println()
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("  SCENARIO: %s\n", name)
	fmt.Printf("  %s\n", description)
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
}

// PrintStats prints operation statistics in a formatted table
func PrintStats(stats map[OpType]*OpStats) {
	fmt.Println()
	fmt.Printf("  %-20s %8s %8s %8s %8s %8s %10s %10s\n",
		"Operation", "Count", "P50ms", "P95ms", "P99ms", "Errors", "MB/s", "ops/s")
	fmt.Printf("  %s\n", strings.Repeat("─", 88))

	knownOps := []OpType{
		OpPutSmall, OpPutLarge, OpCreateMultipart, OpUploadPart,
		OpCompleteMultipart, OpAbortMultipart, OpGetFull, OpGetRange,
		OpHeadObject, OpDeleteObjects, OpDeleteObject, OpListObjects, OpListMultipart,
	}
	for _, op := range knownOps {
		st, ok := stats[op]
		if !ok || st.Count == 0 {
			continue
		}
		fmt.Printf("  %-20s %8d %8.1f %8.1f %8.1f %8d %10.1f %10.1f\n",
			st.OpType, st.Count, st.P50Ms, st.P95Ms, st.P99Ms,
			st.ErrorCount, st.ThroughputMBps, st.OpsPerSec)
	}

	// Print IOPS stats (from iops scenario) that use string keys
	knownSet := make(map[OpType]bool)
	for _, op := range knownOps {
		knownSet[op] = true
	}
	for key, st := range stats {
		if knownSet[key] || st.Count == 0 {
			continue
		}
		fmt.Printf("  %-20s %8d %8.1f %8s %8.1f %8d %10s %10.1f\n",
			st.OpType, st.Count, st.P50Ms, "-", st.P99Ms,
			st.ErrorCount, "-", st.OpsPerSec)
	}
	fmt.Println()
}

// PrintChecks prints pass/fail checks
func PrintChecks(checks []Check) {
	if len(checks) == 0 {
		return
	}
	fmt.Printf("  Checks:\n")
	for _, c := range checks {
		icon := "✓"
		if !c.Passed {
			icon := "✗"
			_ = icon
		}
		if !c.Passed {
			icon = "✗"
		}
		fmt.Printf("    %s %-50s %s\n", icon, c.Name, c.Detail)
	}
}

// PrintVerdict prints the final verdict
func PrintVerdict(report *FullReport) {
	fmt.Println()
	fmt.Printf("╔══════════════════════════════════════════════════════════════╗\n")

	switch report.Verdict {
	case "PASS":
		fmt.Printf("║               VERDICT: PASS                                ║\n")
	case "FAIL":
		fmt.Printf("║               VERDICT: FAIL                                ║\n")
	default:
		fmt.Printf("║               VERDICT: WARN                                ║\n")
	}

	fmt.Printf("╠══════════════════════════════════════════════════════════════╣\n")

	lines := wrapText(report.Summary, 58)
	for _, line := range lines {
		fmt.Printf("║  %-58s║\n", line)
	}

	fmt.Printf("╠══════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Endpoint: %-47s║\n", truncate(report.Endpoint, 47))
	fmt.Printf("║  Bucket:   %-47s║\n", report.Bucket)
	fmt.Printf("║  Role:     %-47s║\n", report.Role)
	fmt.Printf("║  Duration: %-47s║\n", report.TotalTime.Round(time.Second))

	totalChecks, passedChecks := 0, 0
	for _, s := range report.Scenarios {
		for _, c := range s.Checks {
			totalChecks++
			if c.Passed {
				passedChecks++
			}
		}
	}
	fmt.Printf("║  Checks:   %d/%d passed%-33s║\n", passedChecks, totalChecks, "")

	fmt.Printf("╚══════════════════════════════════════════════════════════════╝\n")
}

// WriteJSONReport writes the full report as JSON
func WriteJSONReport(report *FullReport, filename string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}

	if filename == "" || filename == "-" {
		fmt.Println(string(data))
		return nil
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("write report file: %w", err)
	}
	fmt.Printf("\nJSON report written to: %s\n", filename)
	return nil
}

// ComputeVerdict analyzes all results and returns PASS/FAIL/WARN
func ComputeVerdict(results []*ScenarioResult) (verdict, summary string) {
	totalChecks := 0
	passedChecks := 0
	failedNames := []string{}

	for _, r := range results {
		for _, c := range r.Checks {
			totalChecks++
			if c.Passed {
				passedChecks++
			} else {
				failedNames = append(failedNames, c.Name)
			}
		}
	}

	if totalChecks == 0 {
		return "WARN", "No checks were executed"
	}

	passRate := float64(passedChecks) / float64(totalChecks) * 100

	if passedChecks == totalChecks {
		return "PASS", fmt.Sprintf(
			"All %d checks passed. This S3 backend is compatible with ClickHouse workloads.",
			totalChecks)
	}

	if passRate >= 80 {
		return "WARN", fmt.Sprintf(
			"%d/%d checks passed (%.0f%%). Some thresholds exceeded. "+
				"Review: %s",
			passedChecks, totalChecks, passRate, strings.Join(failedNames, ", "))
	}

	return "FAIL", fmt.Sprintf(
		"%d/%d checks passed (%.0f%%). This S3 backend may NOT be compatible with ClickHouse. "+
			"Failed: %s",
		passedChecks, totalChecks, passRate, strings.Join(failedNames, ", "))
}

func wrapText(text string, width int) []string {
	words := strings.Fields(text)
	var lines []string
	current := ""

	for _, word := range words {
		if current == "" {
			current = word
		} else if len(current)+1+len(word) <= width {
			current += " " + word
		} else {
			lines = append(lines, current)
			current = word
		}
	}
	if current != "" {
		lines = append(lines, current)
	}
	return lines
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

type referenceRange struct {
	label string
	min   float64
	max   float64
	unit  string
}

// PrintValidationGuide prints and returns a validation guide comparing measured
// values against known-good reference ranges for the given preset.
func PrintValidationGuide(report *FullReport, preset string) []ValidationResult {
	fmt.Println()
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Validation Guide — preset: %s\n", preset)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	var ranges []referenceRange

	switch preset {
	case "s3-baseline":
		fmt.Println("  Reference: cloud object storage (high-latency, unlimited IOPS)")
		fmt.Println()
		ranges = []referenceRange{
			{"Peak GET IOPS (ch-realistic 64KB)", 50000, 999999, "IOPS"},
			{"Peak PUT IOPS (ch-realistic 1MB)", 3500, 999999, "IOPS"},
			{"GET P99 latency @ operating load", 80, 300, "ms"},
			{"PUT P99 latency @ operating load", 150, 800, "ms"},
			{"Mixed scenario total IOPS", 800, 2500, "IOPS"},
			{"Idle connection resets (61s)", 0, 0, "resets"},
			{"Consistency violations", 0, 0, "violations"},
			{"TTFB 64KB GET P99", 20, 200, "ms"},
		}

	case "minio-hdd":
		fmt.Println("  Reference: local MinIO cluster on spinning disks (LAN, IOPS-bounded)")
		fmt.Println()
		ranges = []referenceRange{
			{"Peak GET IOPS (raw 4KB)", 15000, 21600, "IOPS"},
			{"Peak GET IOPS (ch-realistic 64KB)", 3000, 8000, "IOPS"},
			{"Peak PUT IOPS (ch-realistic 1MB)", 300, 800, "IOPS"},
			{"Peak GET throughput", 200, 600, "MB/s"},
			{"Peak PUT throughput", 150, 400, "MB/s"},
			{"GET P99 latency @ operating load", 2, 20, "ms"},
			{"PUT P99 latency @ operating load", 20, 200, "ms"},
			{"GET saturation concurrency", 50, 300, "threads"},
			{"Mixed scenario total IOPS", 600, 2000, "IOPS"},
			{"Idle connection resets (61s)", 0, 0, "resets"},
			{"Consistency violations", 0, 0, "violations"},
			{"TTFB 64KB GET P99", 1, 15, "ms"},
		}
	}

	fmt.Printf("  %-48s %-20s %s\n", "Metric", "Expected range", "Status")
	fmt.Printf("  %s\n", strings.Repeat("─", 90))

	measured := extractMeasuredValues(report)
	var results []ValidationResult

	for _, r := range ranges {
		val, ok := measured[r.label]
		status := "N/A"
		marker := "·"
		rangeStr := formatRange(r)

		if ok {
			if r.min == 0 && r.max == 0 {
				if val == 0 {
					status = "PASS"
					marker = "✓"
				} else {
					status = fmt.Sprintf("FAIL (%.0f)", val)
					marker = "✗"
				}
			} else if val >= r.min && (r.max >= 999990 || val <= r.max) {
				status = "PASS"
				marker = "✓"
			} else if val < r.min {
				status = fmt.Sprintf("LOW (%.0f, need >= %.0f)", val, r.min)
				marker = "✗"
			} else {
				status = fmt.Sprintf("HIGH (%.0f, need <= %.0f)", val, r.max)
				marker = "✗"
			}
		}

		fmt.Printf("  %s %-48s %-20s %s\n", marker, r.label, rangeStr, status)
		results = append(results, ValidationResult{
			Label:    r.label,
			Expected: rangeStr,
			Measured: val,
			HasData:  ok,
			Status:   status,
		})
	}

	fmt.Println()
	fmt.Println("  Capacity planner check (3 replicas x 236 GiB):")
	printCapacityCheck(report, 3, 236)
	fmt.Println("  Capacity planner check (6 replicas x 236 GiB):")
	printCapacityCheck(report, 6, 236)
	fmt.Println()

	return results
}

func formatRange(r referenceRange) string {
	if r.min == 0 && r.max == 0 {
		return fmt.Sprintf("= 0 %s", r.unit)
	}
	if r.max >= 999990 {
		return fmt.Sprintf(">= %.0f %s", r.min, r.unit)
	}
	return fmt.Sprintf("%.0f – %.0f %s", r.min, r.max, r.unit)
}

// extractMeasuredValues pulls relevant numbers from the report into a flat map
// keyed by the metric labels used in the reference ranges.
func extractMeasuredValues(report *FullReport) map[string]float64 {
	m := make(map[string]float64)

	for _, s := range report.Scenarios {
		if s.StorageCapabilities != nil {
			caps := s.StorageCapabilities
			m["Peak GET IOPS (ch-realistic 64KB)"] = caps.PeakGetIOPS
			m["Peak PUT IOPS (ch-realistic 1MB)"] = caps.PeakPutIOPS
			m["GET P99 latency @ operating load"] = caps.GetP99AtPeakMs
			m["PUT P99 latency @ operating load"] = caps.PutP99AtPeakMs
			m["Peak GET throughput"] = caps.PeakGetMBps
			m["Peak PUT throughput"] = caps.PeakPutMBps
			m["GET saturation concurrency"] = float64(caps.GetSaturationThreads)
		}

		// Raw 4KB GET IOPS from IOPS scenario stats
		for key, st := range s.Stats {
			if string(key) == "raw-4KB_get_IOPS" && st.OpsPerSec > 0 {
				m["Peak GET IOPS (raw 4KB)"] = st.OpsPerSec
			}
		}

		// Mixed scenario total IOPS
		if s.Name == "MIXED (SharedMergeTree Simulation)" {
			var totalOps int64
			for _, st := range s.Stats {
				totalOps += st.Count
			}
			if s.Duration > 0 {
				m["Mixed scenario total IOPS"] = float64(totalOps) / s.Duration.Seconds()
			}
		}

		// Extract from compat checks
		for _, c := range s.Checks {
			if strings.Contains(c.Name, "Connection idle timeout") || strings.Contains(c.Name, "idle timeout") {
				if c.Passed {
					m["Idle connection resets (61s)"] = 0
				} else {
					m["Idle connection resets (61s)"] = 1
				}
			}
			if strings.Contains(c.Name, "consistency") || strings.Contains(c.Name, "Consistency") {
				if strings.Contains(c.Name, "Read-after-write consistency under concurrency") {
					if c.Passed {
						m["Consistency violations"] = 0
					} else {
						m["Consistency violations"] = 1
					}
				}
			}
			if strings.Contains(c.Name, "TTFB") || strings.Contains(c.Name, "64KB range GET P99") {
				// Parse P99 from detail like "64KB range GET TTFB P50=1.23ms P99=4.56ms ..."
				if idx := strings.Index(c.Detail, "P99="); idx >= 0 {
					var p99val float64
					fmt.Sscanf(c.Detail[idx:], "P99=%fms", &p99val)
					if p99val > 0 {
						m["TTFB 64KB GET P99"] = p99val
					}
				}
			}
		}
	}

	return m
}

// PrintSummaryBlock prints and returns a compact, copy-pasteable result block
// containing all numbers needed to evaluate the storage. Plain text, no color
// codes or box-drawing characters inside the block.
func PrintSummaryBlock(report *FullReport, cfg *Config) string {
	var b strings.Builder
	line := func(format string, args ...interface{}) {
		fmt.Fprintf(&b, format+"\n", args...)
	}

	line("")
	line("================================================================")
	line("  RESULT BLOCK -- copy everything between the lines and share it")
	line("================================================================")
	line("endpoint:       %s", cfg.Endpoint)
	line("region:         %s", cfg.Region)
	line("path_style:     %v", cfg.PathStyle)
	if report.Resources != nil {
		line("test_machine:   %s  %d vCPU  %.0f GB RAM",
			report.Resources.Hostname,
			report.Resources.CPUCores,
			report.Resources.TotalRAMGB)
		line("scale_factor:   %.2f", report.Resources.ScaleFactor)
	}

	// Compat results
	line("")
	line("--- compat ---")
	for _, s := range report.Scenarios {
		if s.Name != "S3 API Compatibility" {
			continue
		}
		must, mustPass := 0, 0
		should, shouldPass := 0, 0
		for _, c := range s.Checks {
			if strings.HasPrefix(c.Name, "[MUST]") {
				must++
				if c.Passed {
					mustPass++
				}
			} else if strings.HasPrefix(c.Name, "[SHOULD]") {
				should++
				if c.Passed {
					shouldPass++
				}
			}
		}
		line("must:           %d/%d", mustPass, must)
		line("should:         %d/%d", shouldPass, should)

		if st, ok := s.Stats[OpGetRange]; ok && st.Count > 0 {
			line("ttfb_p50_ms:    %.1f", st.P50Ms)
			line("ttfb_p99_ms:    %.1f", st.P99Ms)
		}
	}

	// IOPS results
	line("")
	line("--- iops ---")
	for _, s := range report.Scenarios {
		if s.Name != "IOPS Capability Discovery" {
			continue
		}

		// Raw IOPS from stats keys
		for key, st := range s.Stats {
			switch string(key) {
			case "raw-4KB_get_IOPS":
				if st.OpsPerSec > 0 {
					line("peak_get_iops_raw:      %.0f", st.OpsPerSec)
				}
			case "raw-4KB_put_IOPS":
				if st.OpsPerSec > 0 {
					line("peak_put_iops_raw:      %.0f", st.OpsPerSec)
				}
			}
		}

		if caps := s.StorageCapabilities; caps != nil {
			line("peak_get_iops_ch:       %.0f", caps.PeakGetIOPS)
			line("peak_put_iops_ch:       %.0f", caps.PeakPutIOPS)
			line("peak_get_mbps:          %.0f", caps.PeakGetMBps)
			line("peak_put_mbps:          %.0f", caps.PeakPutMBps)
			line("get_p99_at_peak_ms:     %.1f", caps.GetP99AtPeakMs)
			line("put_p99_at_peak_ms:     %.1f", caps.PutP99AtPeakMs)
			line("get_saturation_threads: %d", caps.GetSaturationThreads)
			line("put_saturation_threads: %d", caps.PutSaturationThreads)
			line("network_bw_mbps:        %.0f", caps.NetworkBandwidthMBps)
		}

		// Fallback latency from individual stat keys if StorageCapabilities is nil
		if s.StorageCapabilities == nil {
			for key, st := range s.Stats {
				switch string(key) {
				case "ch-realistic_get_IOPS":
					if st.OpsPerSec > 0 {
						line("peak_get_iops_ch:       %.0f", st.OpsPerSec)
						line("get_p50_ms:             %.1f", st.P50Ms)
						line("get_p99_ms:             %.1f", st.P99Ms)
					}
				case "ch-realistic_put_IOPS":
					if st.OpsPerSec > 0 {
						line("peak_put_iops_ch:       %.0f", st.OpsPerSec)
						line("put_p50_ms:             %.1f", st.P50Ms)
						line("put_p99_ms:             %.1f", st.P99Ms)
					}
				}
			}
		}

		// Timeout violations from checks
		for _, c := range s.Checks {
			if strings.Contains(c.Name, "error rate") || strings.Contains(c.Name, "PUT error rate") {
				line("peak_error_rate:        %s", c.Detail)
				break
			}
		}

		// Consistency violations from compat scenario (if available)
		consistencyViolations := 0
		for _, cs := range report.Scenarios {
			for _, c := range cs.Checks {
				if strings.Contains(c.Name, "Read-after-write consistency under concurrency") {
					if !c.Passed {
						consistencyViolations = 1
					}
					break
				}
			}
		}
		line("consistency_violations: %d", consistencyViolations)
	}

	line("")
	line("================================================================")

	text := b.String()
	fmt.Print(text)
	return text
}

func printCapacityCheck(report *FullReport, replicas, ramGiB int) {
	for _, s := range report.Scenarios {
		if s.CapacityReport == nil {
			continue
		}
		for _, cfg := range s.CapacityReport {
			if cfg.Replicas == replicas && cfg.RAMPerReplicaGiB == ramGiB {
				status := "SUPPORTED"
				if !cfg.Supported {
					status = fmt.Sprintf("NOT SUPPORTED — limit: %s", cfg.LimitingFactor)
				}
				fmt.Printf("    %d x %d GiB: %s (headroom %.0f%%)\n",
					replicas, ramGiB, status, cfg.HeadroomPct)
				return
			}
		}
	}
	fmt.Printf("    %d x %d GiB: data not available\n", replicas, ramGiB)
}
