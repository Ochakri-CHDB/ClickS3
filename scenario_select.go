package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type SelectScenario struct {
	client  *S3Client
	cfg     *Config
	metrics *MetricsCollector
}

func NewSelectScenario(client *S3Client, cfg *Config, metrics *MetricsCollector) *SelectScenario {
	return &SelectScenario{client: client, cfg: cfg, metrics: metrics}
}

func (s *SelectScenario) Name() string { return "SELECT Concurrent" }

func (s *SelectScenario) Description() string {
	return "Simulates ClickHouse OLAP query workload: high-concurrency 64KB range GETs"
}

func (s *SelectScenario) Run(ctx context.Context) (*ScenarioResult, error) {
	fmt.Printf("  ├─ Query threads: %d\n", s.cfg.SelectThreads)
	fmt.Printf("  ├─ Pattern: 3 parts × 30 granules × 3 columns = 276 GETs per query\n")
	fmt.Printf("  ├─ Granule size: 64 KB (8192 rows × ~8 bytes)\n")
	fmt.Printf("  ├─ Duration: %s\n", s.cfg.Duration)

	numParts := 50
	fmt.Printf("  ├─ Setup: creating %d data parts...\n", numParts)
	parts, err := s.createReadParts(ctx, numParts)
	if err != nil {
		return nil, fmt.Errorf("setup failed: %w", err)
	}
	fmt.Printf("  ├─ Created %d parts with %d total objects\n", len(parts), countObjects(parts))

	s.metrics.Reset()

	testCtx, testCancel := context.WithTimeout(ctx, s.cfg.Duration)
	defer testCancel()

	var totalQueries atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < s.cfg.SelectThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.queryWorker(testCtx, parts, &totalQueries)
		}()
	}
	wg.Wait()

	stats := s.metrics.GetAllStats()
	result := &ScenarioResult{
		Name:     s.Name(),
		Duration: s.cfg.Duration,
		Stats:    stats,
		Checks:   s.evaluate(stats),
	}

	fmt.Printf("  ├─ Total queries simulated: %d\n", totalQueries.Load())
	fmt.Printf("  └─ Done\n")
	return result, nil
}

func (s *SelectScenario) createReadParts(ctx context.Context, count int) ([][]PartFile, error) {
	var mu sync.Mutex
	var allParts [][]PartFile
	var wg sync.WaitGroup
	sem := make(chan struct{}, 20)

	for i := 0; i < count; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			dataSize := randBetween(2*1024*1024, 10*1024*1024)
			part := GenerateCompactPart(s.cfg.Prefix, dataSize)
			var putWg sync.WaitGroup
			for _, f := range part {
				putWg.Add(1)
				go func(pf PartFile) {
					defer putWg.Done()
					s.client.PutObject(ctx, pf.Key, pf.Size)
				}(f)
			}
			putWg.Wait()
			mu.Lock()
			allParts = append(allParts, part)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return allParts, nil
}

func (s *SelectScenario) queryWorker(ctx context.Context, parts [][]PartFile, totalQueries *atomic.Int64) {
	partsToScan := 3
	granulesPerPart := 30

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var queryWg sync.WaitGroup

		for p := 0; p < partsToScan; p++ {
			// 60% cache-cold random, 40% hot re-read
			var partIdx int
			if randBetween(0, 100) < 40 {
				partIdx = 0
			} else {
				partIdx = int(randBetween(0, int64(len(parts))))
			}
			part := parts[partIdx]

			for _, f := range part {
				if f.FileName == "primary.idx" {
					queryWg.Add(1)
					go func(pf PartFile) {
						defer queryWg.Done()
						s.client.GetObject(ctx, pf.Key)
					}(f)
					break
				}
			}

			for _, f := range part {
				if f.FileName == "data.mrk3" {
					queryWg.Add(1)
					go func(pf PartFile) {
						defer queryWg.Done()
						s.client.GetObject(ctx, pf.Key)
					}(f)
					break
				}
			}

			// Range reads on data.bin — 64 KB granules (CH granule = 8192 rows)
			for _, f := range part {
				if f.FileName == "data.bin" {
					for g := 0; g < granulesPerPart; g++ {
						queryWg.Add(1)
						go func(pf PartFile, granule int) {
							defer queryWg.Done()
							offset := int64(granule) * 64 * 1024
							if offset+64*1024 > pf.Size {
								offset = 0
							}
							s.client.GetObjectRange(ctx, pf.Key, offset, 64*1024)
						}(f, g)
					}
					break
				}
			}
		}

		queryWg.Wait()
		totalQueries.Add(1)
	}
}

func (s *SelectScenario) evaluate(stats map[OpType]*OpStats) []Check {
	var checks []Check

	req := CHRequirements
	if st, ok := stats[OpGetRange]; ok && st.Count > 0 {
		checks = append(checks,
			checkLatencyPass("GET range P50 (first byte)", st.P50Ms, req.GetRangeP50MaxMs),
			checkLatencyPass("GET range P99", st.P99Ms, req.GetRangeP99MaxMs),
		)
	}
	if st, ok := stats[OpGetFull]; ok && st.Count > 0 {
		checks = append(checks,
			checkLatencyPass("GET full (index/marks) P99", st.P99Ms, req.GetFullP99MaxMs),
		)
	}

	timeouts := s.metrics.TimeoutViolations.Load()
	checks = append(checks, Check{
		Name:   "No requests > 30s (CH timeout threshold)",
		Passed: timeouts == 0,
		Detail: fmt.Sprintf("%d violations", timeouts),
	})

	return checks
}
