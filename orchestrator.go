package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Orchestrator deploys clicks3 to remote nodes, runs benchmarks, collects reports.
type Orchestrator struct {
	cfg        *Config
	nodes      []string
	sshKey     string
	sshUser    string
	localBin   string
	remoteBin  string
	reportDir  string
}

func NewOrchestrator(cfg *Config) *Orchestrator {
	return &Orchestrator{
		cfg:       cfg,
		nodes:     cfg.Nodes,
		sshKey:    cfg.SSHKey,
		sshUser:   cfg.SSHUser,
		remoteBin: "/tmp/clicks3",
		reportDir: fmt.Sprintf("/tmp/clicks3-reports-%d", time.Now().Unix()),
	}
}

func (o *Orchestrator) Run() int {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║     ClickS3 — Distributed Orchestrator                     ║")
	fmt.Println("║     Deploy → Run → Collect → Merge (one command)           ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Nodes:    %s\n", strings.Join(o.nodes, ", "))
	fmt.Printf("  SSH key:  %s\n", o.sshKey)
	fmt.Printf("  SSH user: %s\n", o.sshUser)
	fmt.Println()

	if err := o.crossCompile(); err != nil {
		o.errf("Cross-compile failed: %v", err)
		return 1
	}

	if err := o.deploy(); err != nil {
		o.errf("Deploy failed: %v", err)
		return 1
	}

	if err := o.runBenchmarks(); err != nil {
		o.errf("Benchmark failed: %v", err)
		return 1
	}

	reports, err := o.collectReports()
	if err != nil {
		o.errf("Collect reports failed: %v", err)
		return 1
	}

	o.printCombinedReport(reports)

	o.cleanup()

	return o.exitCode(reports)
}

// crossCompile builds clicks3 for linux/amd64 (the common server target).
func (o *Orchestrator) crossCompile() error {
	o.logf("Cross-compiling clicks3 for linux/amd64...")

	binPath := filepath.Join(os.TempDir(), "clicks3-linux-amd64")

	// If already running on linux/amd64, use current binary
	selfBin, _ := os.Executable()
	if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" && selfBin != "" {
		o.localBin = selfBin
		o.okf("Using current binary: %s", selfBin)
		return nil
	}

	cmd := exec.Command("go", "build", "-ldflags", "-s -w", "-o", binPath, ".")
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")
	cmd.Dir = findProjectDir()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go build failed: %w", err)
	}

	o.localBin = binPath
	info, _ := os.Stat(binPath)
	o.okf("Binary built: %s (%.1f MB)", binPath, float64(info.Size())/(1024*1024))
	return nil
}

// deploy copies the binary to all nodes in parallel.
func (o *Orchestrator) deploy() error {
	o.logf("Deploying to %d nodes...", len(o.nodes))

	var wg sync.WaitGroup
	errs := make([]error, len(o.nodes))

	for i, node := range o.nodes {
		wg.Add(1)
		go func(idx int, target string) {
			defer wg.Done()

			host := o.sshTarget(target)
			o.logf("  → %s", host)

			if err := o.scp(o.localBin, host+":"+o.remoteBin); err != nil {
				errs[idx] = fmt.Errorf("scp to %s: %w", host, err)
				return
			}

			if err := o.ssh(host, "chmod +x "+o.remoteBin); err != nil {
				errs[idx] = fmt.Errorf("chmod on %s: %w", host, err)
				return
			}
		}(i, node)
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	o.okf("Binary deployed to all %d nodes", len(o.nodes))
	return nil
}

// runBenchmarks runs clicks3 on all nodes in parallel.
func (o *Orchestrator) runBenchmarks() error {
	o.logf("Running benchmark on %d nodes (all run INSERT+MERGE+SELECT)...", len(o.nodes))
	fmt.Println()

	var wg sync.WaitGroup
	errs := make([]error, len(o.nodes))

	for i, node := range o.nodes {
		wg.Add(1)
		go func(idx int, target string) {
			defer wg.Done()

			host := o.sshTarget(target)
			role := fmt.Sprintf("node%d", idx+1)
			nodeID := fmt.Sprintf("bench-%d-%s", idx+1, target)
			remoteReport := fmt.Sprintf("/tmp/clicks3-report-%d.json", idx+1)

			args := o.buildRemoteArgs(role, nodeID, remoteReport)
			cmdStr := o.remoteBin + " " + strings.Join(args, " ")

			o.logf("  [%s] starting %s...", nodeID, role)

			if err := o.sshStream(host, cmdStr); err != nil {
				errs[idx] = fmt.Errorf("node %s: %w", target, err)
				o.errf("  [%s] FAILED: %v", nodeID, err)
			} else {
				o.okf("  [%s] completed", nodeID)
			}
		}(i, node)
	}

	wg.Wait()

	failCount := 0
	for _, err := range errs {
		if err != nil {
			failCount++
		}
	}

	if failCount > 0 {
		o.errf("%d/%d nodes reported errors", failCount, len(o.nodes))
	} else {
		o.okf("All %d nodes completed", len(o.nodes))
	}
	return nil
}

// collectReports fetches JSON reports from all nodes.
func (o *Orchestrator) collectReports() ([]*FullReport, error) {
	o.logf("Collecting reports from %d nodes...", len(o.nodes))

	os.MkdirAll(o.reportDir, 0755)

	var reports []*FullReport
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i, node := range o.nodes {
		wg.Add(1)
		go func(idx int, target string) {
			defer wg.Done()

			host := o.sshTarget(target)
			remoteReport := fmt.Sprintf("/tmp/clicks3-report-%d.json", idx+1)
			localReport := filepath.Join(o.reportDir, fmt.Sprintf("report-node%d.json", idx+1))

			if err := o.scpFrom(host+":"+remoteReport, localReport); err != nil {
				o.errf("  Could not fetch report from %s: %v", target, err)
				return
			}

			data, err := os.ReadFile(localReport)
			if err != nil {
				o.errf("  Could not read %s: %v", localReport, err)
				return
			}

			var report FullReport
			if err := json.Unmarshal(data, &report); err != nil {
				o.errf("  Could not parse report from %s: %v", target, err)
				return
			}

			mu.Lock()
			reports = append(reports, &report)
			mu.Unlock()
		}(i, node)
	}

	wg.Wait()

	o.okf("Collected %d/%d reports → %s/", len(reports), len(o.nodes), o.reportDir)
	return reports, nil
}

// cleanup removes remote binaries and reports.
func (o *Orchestrator) cleanup() {
	o.logf("Cleaning up remote nodes...")

	var wg sync.WaitGroup
	for i, node := range o.nodes {
		wg.Add(1)
		go func(idx int, target string) {
			defer wg.Done()
			host := o.sshTarget(target)
			remoteReport := fmt.Sprintf("/tmp/clicks3-report-%d.json", idx+1)
			o.ssh(host, fmt.Sprintf("rm -f %s %s", o.remoteBin, remoteReport))
		}(i, node)
	}
	wg.Wait()
	o.okf("Remote cleanup done")
}

// printCombinedReport merges and displays results from all nodes.
func (o *Orchestrator) printCombinedReport(reports []*FullReport) {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║     ClickS3 — Combined Multi-Node Report                   ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	totalChecks, passedChecks := 0, 0
	var failedNames []string

	for _, report := range reports {
		icon := "✓"
		if report.Verdict != "PASS" {
			icon = "✗"
		}
		cpu := report.Resources.CPUCores
		ram := report.Resources.TotalRAMGB
		fmt.Printf("  %s %-30s  %-6s  %d vCPU / %.1f GB\n",
			icon, report.NodeID, report.Verdict, cpu, ram)

		for _, s := range report.Scenarios {
			for _, c := range s.Checks {
				totalChecks++
				if c.Passed {
					passedChecks++
				} else {
					failedNames = append(failedNames, fmt.Sprintf("%s: %s", report.NodeID, c.Name))
				}
			}
		}
	}

	fmt.Println()

	// Aggregated stats
	opStats := make(map[string]*aggStat)
	for _, report := range reports {
		for _, s := range report.Scenarios {
			for opType, st := range s.Stats {
				key := string(opType)
				agg, ok := opStats[key]
				if !ok {
					agg = &aggStat{}
					opStats[key] = agg
				}
				agg.count += st.Count
				agg.errors += st.ErrorCount
				agg.throughput += st.ThroughputMBps
				if st.P50Ms > 0 {
					agg.p50s = append(agg.p50s, st.P50Ms)
				}
				if st.P99Ms > 0 {
					agg.p99s = append(agg.p99s, st.P99Ms)
				}
			}
		}
	}

	fmt.Printf("  %-20s %10s %8s %10s %10s %10s\n",
		"Operation", "Total Ops", "Errors", "Avg P50ms", "Avg P99ms", "Total MB/s")
	fmt.Printf("  %s\n", strings.Repeat("─", 72))

	for _, key := range sortedKeys(opStats) {
		agg := opStats[key]
		if agg.count == 0 {
			continue
		}
		avgP50, avgP99 := agg.avgP50(), agg.avgP99()
		fmt.Printf("  %-20s %10d %8d %10.1f %10.1f %10.1f\n",
			key, agg.count, agg.errors, avgP50, avgP99, agg.throughput)
	}

	// Final verdict
	var verdict, summary string
	if totalChecks == 0 {
		verdict, summary = "WARN", "No checks collected"
	} else if passedChecks == totalChecks {
		verdict = "PASS"
		summary = fmt.Sprintf("All %d checks passed across %d nodes.", totalChecks, len(reports))
	} else if float64(passedChecks)/float64(totalChecks) >= 0.8 {
		verdict = "WARN"
		summary = fmt.Sprintf("%d/%d checks passed (%d%%) across %d nodes.",
			passedChecks, totalChecks, 100*passedChecks/totalChecks, len(reports))
	} else {
		verdict = "FAIL"
		summary = fmt.Sprintf("%d/%d checks passed (%d%%) across %d nodes.",
			passedChecks, totalChecks, 100*passedChecks/totalChecks, len(reports))
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Printf("║             COMBINED VERDICT: %-29s║\n", verdict)
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  %-58s║\n", summary)
	fmt.Printf("║  Nodes: %-50d║\n", len(reports))
	fmt.Printf("║  Checks: %d/%d passed%-37s║\n", passedChecks, totalChecks, "")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	if len(failedNames) > 0 {
		fmt.Println()
		fmt.Println("  Failed checks:")
		limit := 20
		if len(failedNames) < limit {
			limit = len(failedNames)
		}
		for _, name := range failedNames[:limit] {
			fmt.Printf("    ✗ %s\n", name)
		}
		if len(failedNames) > 20 {
			fmt.Printf("    ... and %d more\n", len(failedNames)-20)
		}
	}

	// Write combined JSON
	combined := map[string]interface{}{
		"nodes":         len(reports),
		"total_checks":  totalChecks,
		"passed_checks": passedChecks,
		"verdict":       verdict,
		"summary":       summary,
		"node_reports":  reports,
	}
	combinedPath := filepath.Join(o.reportDir, "combined-report.json")
	if data, err := json.MarshalIndent(combined, "", "  "); err == nil {
		os.WriteFile(combinedPath, data, 0644)
		fmt.Printf("\n  Reports: %s/\n", o.reportDir)
	}
}

func (o *Orchestrator) exitCode(reports []*FullReport) int {
	for _, r := range reports {
		if r.Verdict == "FAIL" {
			return 1
		}
	}
	return 0
}

// buildRemoteArgs constructs the clicks3 CLI arguments for a remote node.
func (o *Orchestrator) buildRemoteArgs(role, nodeID, outputFile string) []string {
	args := []string{
		"--role", role,
		"--node-id", nodeID,
		"--output", outputFile,
		"--bucket", o.cfg.Bucket,
		"--region", o.cfg.Region,
		"--prefix", o.cfg.Prefix,
		"--duration", o.cfg.Duration.String(),
		"--warmup", o.cfg.WarmupDuration.String(),
		"--request-timeout", fmt.Sprintf("%d", o.cfg.RequestTimeoutMs),
		"--connect-timeout", fmt.Sprintf("%d", o.cfg.ConnectTimeoutMs),
		"--insert-threads", fmt.Sprintf("%d", o.cfg.InsertThreads),
		"--merge-threads", fmt.Sprintf("%d", o.cfg.MergeThreads),
		"--select-threads", fmt.Sprintf("%d", o.cfg.SelectThreads),
	}

	if o.cfg.MaxConcurrency != 2000 {
		args = append(args, "--max-concurrency", fmt.Sprintf("%d", o.cfg.MaxConcurrency))
	}
	if o.cfg.ScenarioPreset != "" {
		args = append(args, "--scenario-preset", o.cfg.ScenarioPreset)
	}
	if o.cfg.ReportSummary {
		args = append(args, "--report-summary")
	}

	if o.cfg.Endpoint != "" {
		args = append(args, "--endpoint", o.cfg.Endpoint)
	}
	if o.cfg.AccessKey != "" {
		args = append(args, "--access-key", o.cfg.AccessKey)
	}
	if o.cfg.SecretKey != "" {
		args = append(args, "--secret-key", o.cfg.SecretKey)
	}
	if o.cfg.SessionToken != "" {
		args = append(args, "--session-token", o.cfg.SessionToken)
	}
	if o.cfg.PathStyle {
		args = append(args, "--path-style=true")
	}
	if o.cfg.TLSSkipVerify {
		args = append(args, "--tls-skip-verify")
	}
	if o.cfg.TLSCACert != "" {
		args = append(args, "--tls-ca-cert", o.cfg.TLSCACert)
	}
	if !o.cfg.AutoScale {
		args = append(args, "--auto-scale=false")
	}

	return args
}

// SSH/SCP helpers using the system ssh/scp commands.
func (o *Orchestrator) sshTarget(node string) string {
	if strings.Contains(node, "@") {
		return node
	}
	return o.sshUser + "@" + node
}

func (o *Orchestrator) sshOpts() []string {
	opts := []string{
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "ConnectTimeout=15",
		"-o", "LogLevel=ERROR",
	}
	if o.sshKey != "" {
		opts = append(opts, "-i", o.sshKey)
	}
	return opts
}

func (o *Orchestrator) ssh(host, command string) error {
	args := append(o.sshOpts(), host, command)
	cmd := exec.Command("ssh", args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	return cmd.Run()
}

func (o *Orchestrator) sshStream(host, command string) error {
	args := append(o.sshOpts(), host, command)
	cmd := exec.Command("ssh", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (o *Orchestrator) scp(src, dst string) error {
	args := append(o.sshOpts(), src, dst)
	cmd := exec.Command("scp", args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	return cmd.Run()
}

func (o *Orchestrator) scpFrom(src, dst string) error {
	args := append(o.sshOpts(), src, dst)
	cmd := exec.Command("scp", args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	return cmd.Run()
}

func (o *Orchestrator) logf(format string, args ...interface{}) {
	fmt.Printf("\033[1;34m[clicks3]\033[0m "+format+"\n", args...)
}

func (o *Orchestrator) okf(format string, args ...interface{}) {
	fmt.Printf("\033[1;32m[  OK  ]\033[0m "+format+"\n", args...)
}

func (o *Orchestrator) errf(format string, args ...interface{}) {
	fmt.Printf("\033[1;31m[ERROR]\033[0m "+format+"\n", args...)
}

// findProjectDir finds the directory containing go.mod.
func findProjectDir() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "."
}

type aggStat struct {
	count      int64
	errors     int64
	throughput float64
	p50s       []float64
	p99s       []float64
}

func (a *aggStat) avgP50() float64 {
	if len(a.p50s) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range a.p50s {
		sum += v
	}
	return sum / float64(len(a.p50s))
}

func (a *aggStat) avgP99() float64 {
	if len(a.p99s) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range a.p99s {
		sum += v
	}
	return sum / float64(len(a.p99s))
}

func sortedKeys(m map[string]*aggStat) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Simple sort
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}
