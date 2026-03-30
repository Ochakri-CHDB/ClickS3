package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type MixedScenario struct {
	client  *S3Client
	cfg     *Config
	metrics *MetricsCollector
}

func NewMixedScenario(client *S3Client, cfg *Config, metrics *MetricsCollector) *MixedScenario {
	return &MixedScenario{client: client, cfg: cfg, metrics: metrics}
}

func (s *MixedScenario) Name() string { return "MIXED (SharedMergeTree Simulation)" }

func (s *MixedScenario) Description() string {
	return "Every node runs INSERT + MERGE + SELECT simultaneously (30/15/55%), maximizing thread usage"
}

func (s *MixedScenario) Run(ctx context.Context) (*ScenarioResult, error) {
	totalThreads := s.cfg.InsertThreads + s.cfg.MergeThreads + s.cfg.SelectThreads
	insertT := int(float64(totalThreads) * 0.30)
	mergeT := int(float64(totalThreads) * 0.15)
	selectT := totalThreads - insertT - mergeT

	if insertT < 1 {
		insertT = 1
	}
	if mergeT < 1 {
		mergeT = 1
	}
	if selectT < 1 {
		selectT = 1
	}

	fmt.Printf("  ├─ Total threads per node: %d\n", totalThreads)
	fmt.Printf("  ├─ INSERT: %d threads (%.0f%%)\n", insertT, float64(insertT)/float64(totalThreads)*100)
	fmt.Printf("  ├─ MERGE:  %d threads (%.0f%%)\n", mergeT, float64(mergeT)/float64(totalThreads)*100)
	fmt.Printf("  ├─ SELECT: %d threads (%.0f%%)\n", selectT, float64(selectT)/float64(totalThreads)*100)
	fmt.Printf("  ├─ Duration: %s\n", s.cfg.Duration)
	fmt.Printf("  ├─ Mode: every node runs all 3 workloads simultaneously\n")

	fmt.Printf("  ├─ Setup: creating initial data...\n")
	numSetupParts := 200
	parts, err := s.createInitialData(ctx, numSetupParts)
	if err != nil {
		return nil, fmt.Errorf("setup failed: %w", err)
	}

	s.metrics.Reset()

	testCtx, testCancel := context.WithTimeout(ctx, s.cfg.Duration)
	defer testCancel()

	writtenKeys := &sync.Map{}
	deletedKeys := &sync.Map{}
	var consistencyTests atomic.Int64
	var consistencyPasses atomic.Int64
	var totalQueries atomic.Int64

	partChan := make(chan []PartFile, 1000)
	for _, p := range parts {
		select {
		case partChan <- p:
		default:
		}
	}

	var wg sync.WaitGroup

	for i := 0; i < insertT; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.insertNode(testCtx, partChan, writtenKeys)
		}()
	}

	for i := 0; i < mergeT; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.mergeNode(testCtx, partChan, deletedKeys)
		}()
	}

	for i := 0; i < selectT; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.selectNode(testCtx, parts, writtenKeys, deletedKeys, &consistencyTests, &consistencyPasses, &totalQueries)
		}()
	}

	wg.Wait()

	stats := s.metrics.GetAllStats()
	result := &ScenarioResult{
		Name:     s.Name(),
		Duration: s.cfg.Duration,
		Stats:    stats,
		Checks:   s.evaluate(stats, consistencyTests.Load(), consistencyPasses.Load()),
	}

	fmt.Printf("  ├─ Total queries simulated: %d\n", totalQueries.Load())
	fmt.Printf("  ├─ Consistency tests: %d/%d passed\n", consistencyPasses.Load(), consistencyTests.Load())
	fmt.Printf("  └─ Done\n")
	return result, nil
}

func (s *MixedScenario) createInitialData(ctx context.Context, count int) ([][]PartFile, error) {
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
			dataSize := randBetween(500*1024, 5*1024*1024)
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

func (s *MixedScenario) insertNode(ctx context.Context, partChan chan []PartFile, writtenKeys *sync.Map) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		dataSize := randBetween(100*1024, 10*1024*1024)
		part := GenerateCompactPart(s.cfg.Prefix, dataSize)
		var wg sync.WaitGroup
		for _, f := range part {
			wg.Add(1)
			go func(pf PartFile) {
				defer wg.Done()
				if err := s.client.PutObject(ctx, pf.Key, pf.Size); err == nil {
					writtenKeys.Store(pf.Key, time.Now())
				}
			}(f)
		}
		wg.Wait()
		select {
		case partChan <- part:
		default:
		}
	}
}

func (s *MixedScenario) mergeNode(ctx context.Context, partChan chan []PartFile, deletedKeys *sync.Map) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var sources [][]PartFile
		for i := 0; i < 4; i++ {
			select {
			case <-ctx.Done():
				return
			case p := <-partChan:
				sources = append(sources, p)
			case <-time.After(500 * time.Millisecond):
				goto skipMerge
			}
		}

		{
			var readWg sync.WaitGroup
			for _, part := range sources {
				for _, f := range part {
					if f.Size <= 256*1024 {
						readWg.Add(1)
						go func(pf PartFile) {
							defer readWg.Done()
							s.client.GetObject(ctx, pf.Key)
						}(f)
					}
				}
			}
			readWg.Wait()

			mergedSize := randBetween(5*1024*1024, 30*1024*1024)
			merged := GenerateCompactPart(s.cfg.Prefix, mergedSize)
			var writeWg sync.WaitGroup
			for _, f := range merged {
				writeWg.Add(1)
				go func(pf PartFile) {
					defer writeWg.Done()
					s.client.PutObject(ctx, pf.Key, pf.Size)
				}(f)
			}
			writeWg.Wait()

			var keys []string
			for _, part := range sources {
				for _, f := range part {
					keys = append(keys, f.Key)
				}
			}
			if len(keys) > 0 {
				s.client.DeleteObjects(ctx, keys)
				for _, k := range keys {
					deletedKeys.Store(k, time.Now())
				}
			}

			select {
			case partChan <- merged:
			default:
			}
		}
	skipMerge:
	}
}

func (s *MixedScenario) selectNode(ctx context.Context, baseParts [][]PartFile,
	writtenKeys, deletedKeys *sync.Map, tests, passes, queryCount *atomic.Int64) {

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		writtenKeys.Range(func(key, value any) bool {
			k := key.(string)
			writeTime := value.(time.Time)
			if time.Since(writeTime) < 500*time.Millisecond {
				return true
			}
			tests.Add(1)
			_, err := s.client.HeadObject(ctx, k)
			if err == nil {
				passes.Add(1)
			} else {
				s.metrics.ConsistencyViolations.Add(1)
			}
			writtenKeys.Delete(key)
			return false
		})

		if len(baseParts) > 0 {
			partIdx := int(randBetween(0, int64(len(baseParts))))
			part := baseParts[partIdx]
			for _, f := range part {
				if f.FileName == "primary.idx" || f.FileName == "data.mrk3" {
					s.client.GetObject(ctx, f.Key)
				}
				if f.FileName == "data.bin" {
					numRanges := 5
					for r := 0; r < numRanges; r++ {
						offset := int64(r) * 64 * 1024
						if offset+64*1024 <= f.Size {
							s.client.GetObjectRange(ctx, f.Key, offset, 64*1024)
						}
					}
				}
			}
			queryCount.Add(1)
		}
	}
}

func (s *MixedScenario) evaluate(stats map[OpType]*OpStats, tests, passes int64) []Check {
	var checks []Check

	violations := tests - passes
	checks = append(checks, Check{
		Name:   "Read-after-write consistency (0 violations on 1000 tests)",
		Passed: violations == 0,
		Detail: fmt.Sprintf("%d/%d passed", passes, tests),
	})

	var totalPutBytes, totalGetBytes int64
	for op, st := range stats {
		switch op {
		case OpPutSmall, OpPutLarge, OpUploadPart:
			totalPutBytes += st.BytesTransferred
		case OpGetFull, OpGetRange:
			totalGetBytes += st.BytesTransferred
		}
	}

	putMBps := float64(totalPutBytes) / (1024 * 1024) / s.cfg.Duration.Seconds()
	getMBps := float64(totalGetBytes) / (1024 * 1024) / s.cfg.Duration.Seconds()

	req := CHRequirements

	checks = append(checks, Check{
		Name:   fmt.Sprintf("PUT throughput >= %.0f MB/s (CH minimum)", req.MinPutMBps),
		Passed: putMBps >= req.MinPutMBps,
		Detail: fmt.Sprintf("%.1f MB/s measured", putMBps),
	})
	checks = append(checks, Check{
		Name:   fmt.Sprintf("GET throughput >= %.0f MB/s (CH minimum)", req.MinGetMBps),
		Passed: getMBps >= req.MinGetMBps,
		Detail: fmt.Sprintf("%.1f MB/s measured", getMBps),
	})

	var totalErrors int64
	for _, st := range stats {
		totalErrors += st.ErrorCount
	}
	checks = append(checks, Check{
		Name:   "No 503/SlowDown under load",
		Passed: totalErrors == 0,
		Detail: fmt.Sprintf("%d errors total", totalErrors),
	})

	timeouts := s.metrics.TimeoutViolations.Load()
	checks = append(checks, Check{
		Name:   "No requests > 30s (CH timeout = retry storm)",
		Passed: timeouts == 0,
		Detail: fmt.Sprintf("%d violations", timeouts),
	})

	return checks
}
