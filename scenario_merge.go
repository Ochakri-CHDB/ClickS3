package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type MergeScenario struct {
	client  *S3Client
	cfg     *Config
	metrics *MetricsCollector
}

func NewMergeScenario(client *S3Client, cfg *Config, metrics *MetricsCollector) *MergeScenario {
	return &MergeScenario{client: client, cfg: cfg, metrics: metrics}
}

func (s *MergeScenario) Name() string { return "MERGE Continuous" }

func (s *MergeScenario) Description() string {
	return "Simulates ClickHouse background merge: READ sources → WRITE merged → DELETE sources"
}

func (s *MergeScenario) Run(ctx context.Context) (*ScenarioResult, error) {
	fmt.Printf("  ├─ Merge threads: %d (CH background_pool_size=%d)\n", s.cfg.MergeThreads, s.cfg.BackgroundPoolSize)
	fmt.Printf("  ├─ Parts per merge: 4 sources → 1 merged\n")
	fmt.Printf("  ├─ Ratio: ~200 GET + 15 PUT + 1 DeleteObjects per merge cycle\n")
	fmt.Printf("  ├─ Duration: %s\n", s.cfg.Duration)

	// Pre-create objects to merge
	fmt.Printf("  ├─ Setup: creating initial data parts...\n")
	initialParts := s.cfg.MergeThreads * 8
	partSets, err := s.createInitialParts(ctx, initialParts)
	if err != nil {
		return nil, fmt.Errorf("setup failed: %w", err)
	}
	fmt.Printf("  ├─ Created %d initial parts (%d total objects)\n", len(partSets), countObjects(partSets))

	s.metrics.Reset()

	var totalMerges atomic.Int64
	partChan := make(chan []PartFile, len(partSets))
	for _, ps := range partSets {
		partChan <- ps
	}

	testCtx, testCancel := context.WithTimeout(ctx, s.cfg.Duration)
	defer testCancel()

	var wg sync.WaitGroup
	for i := 0; i < s.cfg.MergeThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.mergeWorker(testCtx, partChan, &totalMerges)
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

	fmt.Printf("  ├─ Total merge cycles: %d\n", totalMerges.Load())
	fmt.Printf("  └─ Done\n")
	return result, nil
}

func (s *MergeScenario) createInitialParts(ctx context.Context, count int) ([][]PartFile, error) {
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
			dataSize := randBetween(100*1024, 5*1024*1024)
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

func (s *MergeScenario) mergeWorker(ctx context.Context, partChan chan []PartFile, totalMerges *atomic.Int64) {
	const partsPerMerge = 4

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var sourceParts [][]PartFile
		for i := 0; i < partsPerMerge; i++ {
			select {
			case <-ctx.Done():
				return
			case part := <-partChan:
				sourceParts = append(sourceParts, part)
			default:
				dataSize := randBetween(100*1024, 5*1024*1024)
				part := GenerateCompactPart(s.cfg.Prefix, dataSize)
				var wg sync.WaitGroup
				for _, f := range part {
					wg.Add(1)
					go func(pf PartFile) {
						defer wg.Done()
						s.client.PutObject(ctx, pf.Key, pf.Size)
					}(f)
				}
				wg.Wait()
				sourceParts = append(sourceParts, part)
			}
		}

		if ctx.Err() != nil {
			return
		}

		// Phase 1: Read all source parts (CH uses max_download_threads=4 per file)
		var readWg sync.WaitGroup
		for _, part := range sourceParts {
			for _, f := range part {
				readWg.Add(1)
				go func(pf PartFile) {
					defer readWg.Done()
					if pf.Size > 64*1024 {
						numRanges := int(pf.Size / (64 * 1024))
						if numRanges > 20 {
							numRanges = 20
						}
						for r := 0; r < numRanges; r++ {
							offset := int64(r) * 64 * 1024
							s.client.GetObjectRange(ctx, pf.Key, offset, 64*1024)
						}
					} else {
						s.client.GetObject(ctx, pf.Key)
					}
				}(f)
			}
		}
		readWg.Wait()

		if ctx.Err() != nil {
			return
		}

		// Phase 2: Write merged part
		totalSourceSize := int64(0)
		for _, part := range sourceParts {
			for _, f := range part {
				totalSourceSize += f.Size
			}
		}
		mergedSize := totalSourceSize
		if mergedSize > 200*1024*1024 {
			mergedSize = 200 * 1024 * 1024
		}
		mergedPart := GenerateCompactPart(s.cfg.Prefix, mergedSize)

		var writeWg sync.WaitGroup
		for _, f := range mergedPart {
			writeWg.Add(1)
			go func(pf PartFile) {
				defer writeWg.Done()
				s.client.PutObject(ctx, pf.Key, pf.Size)
			}(f)
		}
		writeWg.Wait()

		// Phase 3: Delete source parts (batch delete, up to 1000 keys)
		var keysToDelete []string
		for _, part := range sourceParts {
			for _, f := range part {
				keysToDelete = append(keysToDelete, f.Key)
			}
		}
		if len(keysToDelete) > 0 {
			s.client.DeleteObjects(ctx, keysToDelete)
		}

		// Verify deletion consistency
		if len(keysToDelete) > 0 {
			_, err := s.client.HeadObject(ctx, keysToDelete[0])
			if err == nil {
				s.metrics.ConsistencyViolations.Add(1)
			}
		}

		select {
		case partChan <- mergedPart:
		default:
		}

		totalMerges.Add(1)
	}
}

func (s *MergeScenario) evaluate(stats map[OpType]*OpStats) []Check {
	var checks []Check

	req := CHRequirements
	if st, ok := stats[OpGetRange]; ok && st.Count > 0 {
		checks = append(checks,
			checkLatencyPass("GET range P50 (merge reads)", st.P50Ms, req.GetRangeP50MaxMs),
			checkLatencyPass("GET range P99 (merge reads)", st.P99Ms, req.GetRangeP99MaxMs),
		)
	}
	if st, ok := stats[OpDeleteObjects]; ok && st.Count > 0 {
		checks = append(checks,
			checkLatencyPass("DeleteObjects P99", st.P99Ms, req.DeleteObjectsP99MaxMs),
		)
	}

	violations := s.metrics.ConsistencyViolations.Load()
	checks = append(checks, Check{
		Name:   "No stale reads after DELETE",
		Passed: violations == 0,
		Detail: fmt.Sprintf("%d consistency violations", violations),
	})

	timeouts := s.metrics.TimeoutViolations.Load()
	checks = append(checks, Check{
		Name:   "No requests > 30s (CH timeout threshold)",
		Passed: timeouts == 0,
		Detail: fmt.Sprintf("%d violations", timeouts),
	})

	return checks
}

func countObjects(parts [][]PartFile) int {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	return total
}
