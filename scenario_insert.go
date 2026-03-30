package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type InsertScenario struct {
	client  *S3Client
	cfg     *Config
	metrics *MetricsCollector
}

func NewInsertScenario(client *S3Client, cfg *Config, metrics *MetricsCollector) *InsertScenario {
	return &InsertScenario{client: client, cfg: cfg, metrics: metrics}
}

func (s *InsertScenario) Name() string { return "INSERT Burst" }

func (s *InsertScenario) Description() string {
	return "Simulates ClickHouse INSERT workload: parallel PUT of compact/wide/large data parts"
}

func (s *InsertScenario) Run(ctx context.Context) (*ScenarioResult, error) {
	totalThreads := s.cfg.InsertThreads
	if s.cfg.Role == "standalone" {
		totalThreads *= 3
	}

	fmt.Printf("  ├─ Threads: %d (role: %s)\n", totalThreads, s.cfg.Role)
	fmt.Printf("  ├─ Duration: %s (+ %s warmup)\n", s.cfg.Duration, s.cfg.WarmupDuration)
	fmt.Printf("  ├─ Mix: 40%% small (<5MB), 40%% medium (5-30MB), 20%% large (>32MB multipart)\n")
	fmt.Printf("  ├─ CH params: max_single_part=32MB, min_part=16MB, inflight=%d\n", s.cfg.MaxInflightParts)

	var totalOps atomic.Int64
	var totalBytes atomic.Int64

	// Warmup
	if s.cfg.WarmupDuration > 0 {
		fmt.Printf("  ├─ Warmup phase (%s)...\n", s.cfg.WarmupDuration)
		warmCtx, warmCancel := context.WithTimeout(ctx, s.cfg.WarmupDuration)
		s.runInsertWorkers(warmCtx, totalThreads/3, nil, nil)
		warmCancel()
		s.metrics.Reset()
	}

	// Measurement
	fmt.Printf("  ├─ Measurement phase (%s)...\n", s.cfg.Duration)
	testCtx, testCancel := context.WithTimeout(ctx, s.cfg.Duration)
	defer testCancel()

	s.runInsertWorkers(testCtx, totalThreads, &totalOps, &totalBytes)

	stats := s.metrics.GetAllStats()
	result := &ScenarioResult{
		Name:     s.Name(),
		Duration: s.cfg.Duration,
		Stats:    stats,
		Checks:   s.evaluate(stats),
	}

	fmt.Printf("  ├─ Total operations: %d\n", totalOps.Load())
	fmt.Printf("  └─ Total data: %.1f MB\n", float64(totalBytes.Load())/(1024*1024))
	return result, nil
}

func (s *InsertScenario) runInsertWorkers(ctx context.Context, numThreads int, totalOps, totalBytes *atomic.Int64) {
	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.insertWorker(ctx, totalOps, totalBytes)
		}()
	}
	wg.Wait()
}

func (s *InsertScenario) insertWorker(ctx context.Context, totalOps, totalBytes *atomic.Int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		dataSize, _ := s.pickPartSize()
		part := GenerateCompactPart(s.cfg.Prefix, dataSize)

		var wg sync.WaitGroup
		for _, f := range part {
			wg.Add(1)
			go func(pf PartFile) {
				defer wg.Done()
				if err := s.client.PutObject(ctx, pf.Key, pf.Size); err != nil {
					if ctx.Err() == nil && s.cfg.Verbose {
						fmt.Printf("    PUT error: %v\n", err)
					}
				}
				if totalBytes != nil {
					totalBytes.Add(pf.Size)
				}
			}(f)
		}
		wg.Wait()

		if totalOps != nil {
			totalOps.Add(1)
		}
	}
}

// pickPartSize returns a data size based on typical ClickHouse distribution:
// ~95% compact parts (< 1GB), ratio singlepart/multipart ~40%/60%
func (s *InsertScenario) pickPartSize() (int64, string) {
	r := randBetween(0, 100)
	switch {
	case r < 40:
		return randBetween(100*1024, 5*1024*1024), "small"
	case r < 80:
		return randBetween(5*1024*1024, 30*1024*1024), "medium"
	default:
		return randBetween(32*1024*1024, 200*1024*1024), "large"
	}
}

func (s *InsertScenario) evaluate(stats map[OpType]*OpStats) []Check {
	var checks []Check

	req := CHRequirements
	if st, ok := stats[OpPutSmall]; ok && st.Count > 0 {
		checks = append(checks,
			checkLatencyPass("PUT singlepart P99", st.P99Ms, req.PutSmallP99MaxMs),
		)
	}
	if st, ok := stats[OpUploadPart]; ok && st.Count > 0 {
		checks = append(checks,
			checkLatencyPass("UploadPart P99", st.P99Ms, req.UploadPartP99MaxMs),
		)
	}
	if st, ok := stats[OpCompleteMultipart]; ok && st.Count > 0 {
		checks = append(checks,
			checkLatencyPass("CompleteMultipart P99", st.P99Ms, req.CompleteMultipartMaxMs),
		)
	}

	// Error rate — ClickHouse expects ~0%
	var totalOps, totalErrors int64
	for _, st := range stats {
		totalOps += st.Count
		totalErrors += st.ErrorCount
	}
	errorRate := float64(0)
	if totalOps > 0 {
		errorRate = float64(totalErrors) / float64(totalOps) * 100
	}
	checks = append(checks, Check{
		Name:   "Error rate < 0.01%",
		Passed: errorRate < 0.01,
		Detail: fmt.Sprintf("%.4f%% (%d/%d)", errorRate, totalErrors, totalOps),
	})

	// Timeout violations — ClickHouse: 0 requests > 30s
	timeouts := s.metrics.TimeoutViolations.Load()
	checks = append(checks, Check{
		Name:   "No requests > 30s (CH timeout threshold)",
		Passed: timeouts == 0,
		Detail: fmt.Sprintf("%d violations", timeouts),
	})

	return checks
}
