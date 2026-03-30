package main

import (
	"context"
	"fmt"
	"os"
)

func main() {
	cfg := ParseConfig()

	if cfg.ShowVersion {
		PrintVersion()
		os.Exit(0)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n\n", err)
		fmt.Fprintf(os.Stderr, "Run 'clicks3 --help' for usage.\n")
		os.Exit(1)
	}

	// Orchestrator mode: deploy + run on remote nodes from one command
	if len(cfg.Nodes) > 0 {
		orch := NewOrchestrator(cfg)
		os.Exit(orch.Run())
	}

	// Local mode: run benchmark on this machine
	metrics := NewMetricsCollector()

	client, err := NewS3Client(cfg, metrics)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create S3 client: %v\n", err)
		os.Exit(1)
	}

	runner := NewRunner(client, cfg, metrics)
	report := runner.Run(context.Background())

	if cfg.OutputFile != "" {
		if err := WriteJSONReport(report, cfg.OutputFile); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write report: %v\n", err)
			os.Exit(1)
		}
	}

	switch report.Verdict {
	case "PASS":
		os.Exit(0)
	case "WARN":
		os.Exit(0)
	case "FAIL":
		os.Exit(1)
	}
}
