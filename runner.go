package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Scenario is the interface all test scenarios implement
type Scenario interface {
	Name() string
	Description() string
	Run(ctx context.Context) (*ScenarioResult, error)
}

// Runner orchestrates scenario execution
type Runner struct {
	client  *S3Client
	cfg     *Config
	metrics *MetricsCollector
}

func NewRunner(client *S3Client, cfg *Config, metrics *MetricsCollector) *Runner {
	return &Runner{client: client, cfg: cfg, metrics: metrics}
}

func (r *Runner) Run(ctx context.Context) *FullReport {
	startTime := time.Now()

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	report := &FullReport{
		Version:   Version,
		Timestamp: startTime,
		Endpoint:  r.cfg.Endpoint,
		Bucket:    r.cfg.Bucket,
		Role:      r.cfg.Role,
		NodeID:    r.cfg.NodeID,
		Resources: r.cfg.Resources,
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Printf("║   ClickS3 v%-48s║\n", Version)
	fmt.Println("║   S3 Storage Capability Discovery for ClickHouse            ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Display server resources
	r.cfg.Resources.PrintBanner()

	if r.cfg.NodeID != "" {
		fmt.Printf("\n  Node ID:      %s\n", r.cfg.NodeID)
	}
	fmt.Println()
	fmt.Printf("  S3 Target:\n")
	if r.cfg.Endpoint != "" {
		fmt.Printf("    Endpoint:     %s\n", r.cfg.Endpoint)
	}
	if r.cfg.Profile != "" {
		fmt.Printf("    AWS Profile:  %s\n", r.cfg.Profile)
	}
	fmt.Printf("    Bucket:       %s\n", r.cfg.Bucket)
	fmt.Printf("    Prefix:       %s\n", r.cfg.Prefix)
	fmt.Printf("    Region:       %s\n", r.cfg.Region)
	if r.cfg.TLSCACert != "" {
		fmt.Printf("    TLS CA:       %s\n", r.cfg.TLSCACert)
	}
	if r.cfg.TLSSkipVerify {
		fmt.Printf("    TLS verify:   DISABLED (insecure)\n")
	}

	fmt.Printf("\n  Test Configuration:\n")
	fmt.Printf("    Role:         %s\n", r.cfg.Role)
	if r.cfg.ScenarioPreset != "" {
		fmt.Printf("    Preset:       %s\n", r.cfg.ScenarioPreset)
	}
	fmt.Printf("    Scenarios:    %v\n", r.cfg.Scenarios)
	fmt.Printf("    Duration:     %s per scenario\n", r.cfg.Duration)
	if r.cfg.AutoScale {
		fmt.Printf("    Auto-scaled:  threads adjusted to %.2fx (based on server resources)\n", r.cfg.Resources.ScaleFactor)
	}
	fmt.Printf("    INSERT:       %d threads\n", r.cfg.InsertThreads)
	fmt.Printf("    MERGE:        %d threads\n", r.cfg.MergeThreads)
	fmt.Printf("    SELECT:       %d threads\n", r.cfg.SelectThreads)

	fmt.Printf("\n  ClickHouse S3 Parameters:\n")
	fmt.Printf("    request_timeout:   %dms (> 30s = retry storm)\n", r.cfg.RequestTimeoutMs)
	fmt.Printf("    connect_timeout:   %dms\n", r.cfg.ConnectTimeoutMs)
	fmt.Printf("    outer_retries:     %d (s3_max_single_read_retries)\n", r.cfg.MaxSingleReadRetries)
	fmt.Printf("    inner_retries:     %d (s3_retry_attempts)\n", r.cfg.MaxRetryAttempts)
	fmt.Printf("    max_single_part:   %d MB\n", r.cfg.MaxSinglePartUploadSize/(1024*1024))
	fmt.Printf("    min_upload_part:   %d MB\n", r.cfg.MinUploadPartSize/(1024*1024))
	fmt.Printf("    max_inflight:      %d parts\n", r.cfg.MaxInflightParts)
	fmt.Println()

	// Ensure bucket exists
	fmt.Print("  Preparing bucket... ")
	if err := r.client.EnsureBucket(ctx); err != nil {
		fmt.Printf("FAILED: %v\n", err)
		fmt.Println("  Continuing anyway (bucket may already exist)...")
	} else {
		fmt.Println("OK")
	}

	// Phase 1: S3 API Compatibility check (runs first, aborts on MUST failure)
	compatAbort := false
	if r.hasCompat() {
		compat := NewCompatScenario(r.client, r.cfg, r.metrics)
		PrintScenarioHeader(compat.Name(), compat.Description())
		r.metrics.Reset()
		result, err := compat.Run(ctx)
		if err != nil {
			fmt.Printf("  ERROR: %v\n", err)
			result = &ScenarioResult{
				Name:   compat.Name(),
				Checks: []Check{{Name: "Compat execution", Passed: false, Detail: err.Error()}},
			}
		}
		report.Scenarios = append(report.Scenarios, result)
		if compat.MustFailed(result) {
			compatAbort = true
			fmt.Printf("\n  ⛔ ABORTING: S3 API missing MUST requirements.\n")
			fmt.Printf("     ClickHouse cannot operate on this storage.\n")
			fmt.Printf("     Skipping throughput/latency benchmarks.\n\n")
		}
		if !r.cfg.NoCleanup {
			r.cleanup(ctx)
		}
	}

	// Phase 2: Throughput/latency benchmarks (skip if compat MUST failed)
	scenarios := r.buildScenarios()

	for i, scenario := range scenarios {
		if ctx.Err() != nil {
			fmt.Printf("\n  Interrupted! Skipping remaining scenarios.\n")
			break
		}

		if compatAbort {
			report.Scenarios = append(report.Scenarios, &ScenarioResult{
				Name:   scenario.Name(),
				Checks: []Check{{Name: "Skipped", Passed: false, Detail: "S3 API compatibility check failed (MUST)"}},
			})
			continue
		}

		PrintScenarioHeader(scenario.Name(), scenario.Description())
		fmt.Printf("  [%d/%d] Starting...\n", i+1, len(scenarios))

		r.metrics.Reset()

		result, err := scenario.Run(ctx)
		if err != nil {
			fmt.Printf("  ERROR: %v\n", err)
			result = &ScenarioResult{
				Name:   scenario.Name(),
				Checks: []Check{{Name: "Scenario execution", Passed: false, Detail: err.Error()}},
			}
		}

		PrintStats(result.Stats)
		PrintChecks(result.Checks)
		report.Scenarios = append(report.Scenarios, result)

		if !r.cfg.NoCleanup && i < len(scenarios)-1 {
			fmt.Printf("  Cleaning up...\n")
			r.cleanup(ctx)
		}
	}

	report.TotalTime = time.Since(startTime)
	report.Verdict, report.Summary = ComputeVerdict(report.Scenarios)

	PrintVerdict(report)

	if r.cfg.ScenarioPreset != "" {
		report.ValidationGuide = PrintValidationGuide(report, r.cfg.ScenarioPreset)
	}

	if r.cfg.ReportSummary {
		report.SummaryBlock = PrintSummaryBlock(report, r.cfg)
	}

	if !r.cfg.NoCleanup {
		fmt.Printf("\n  Final cleanup...\n")
		r.cleanup(ctx)
		fmt.Printf("  Done.\n")
	}

	return report
}

func (r *Runner) hasCompat() bool {
	for _, s := range r.cfg.Scenarios {
		if s == "compat" {
			return true
		}
	}
	return false
}

func (r *Runner) buildScenarios() []Scenario {
	var scenarios []Scenario

	scenarioMap := map[string]func() Scenario{
		"insert": func() Scenario {
			return NewInsertScenario(r.client, r.cfg, r.metrics)
		},
		"merge": func() Scenario {
			return NewMergeScenario(r.client, r.cfg, r.metrics)
		},
		"select": func() Scenario {
			return NewSelectScenario(r.client, r.cfg, r.metrics)
		},
		"mixed": func() Scenario {
			return NewMixedScenario(r.client, r.cfg, r.metrics)
		},
		"failures": func() Scenario {
			return NewFailureScenario(r.client, r.cfg, r.metrics)
		},
		"iops": func() Scenario {
			return NewIOPSScenario(r.client, r.cfg, r.metrics)
		},
	}

	for _, name := range r.cfg.Scenarios {
		if name == "compat" {
			continue // handled separately in Phase 1
		}
		if factory, ok := scenarioMap[name]; ok {
			scenarios = append(scenarios, factory())
		}
	}

	return scenarios
}

func (r *Runner) cleanup(ctx context.Context) {
	cleanCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := r.client.CleanupPrefix(cleanCtx, r.cfg.Prefix); err != nil {
		fmt.Printf("  Cleanup warning: %v\n", err)
	}
}
