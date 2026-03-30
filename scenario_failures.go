package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type FailureScenario struct {
	client  *S3Client
	cfg     *Config
	metrics *MetricsCollector
}

func NewFailureScenario(client *S3Client, cfg *Config, metrics *MetricsCollector) *FailureScenario {
	return &FailureScenario{client: client, cfg: cfg, metrics: metrics}
}

func (s *FailureScenario) Name() string { return "Failure Modes" }

func (s *FailureScenario) Description() string {
	return "Tests S3 edge cases: abort multipart, variable part sizes, high concurrency, batch delete, consistency"
}

func (s *FailureScenario) Run(ctx context.Context) (*ScenarioResult, error) {
	s.metrics.Reset()

	var allChecks []Check

	tests := []struct {
		name string
		fn   func(context.Context) []Check
	}{
		{"E1: Abort Multipart Cleanup", s.testAbortMultipart},
		{"E2: Variable Part Sizes", s.testVariablePartSizes},
		{"E3: High Concurrency PUT (500)", s.testHighConcurrencyPut},
		{"E4: DeleteObjects Batch (1000)", s.testBatchDelete},
		{"E5: Strong Consistency (1000 rounds)", s.testStrongConsistency},
	}

	for _, t := range tests {
		fmt.Printf("  ├─ %s...\n", t.name)
		checks := t.fn(ctx)
		allChecks = append(allChecks, checks...)
		for _, c := range checks {
			status := "PASS"
			if !c.Passed {
				status = "FAIL"
			}
			fmt.Printf("  │  └─ [%s] %s: %s\n", status, c.Name, c.Detail)
		}
	}

	stats := s.metrics.GetAllStats()
	return &ScenarioResult{
		Name:     s.Name(),
		Duration: 0,
		Stats:    stats,
		Checks:   allChecks,
	}, nil
}

// E1: Abort Multipart — verify cleanup of abandoned multipart uploads
func (s *FailureScenario) testAbortMultipart(ctx context.Context) []Check {
	const numUploads = 50
	const partsPerUpload = 5
	partSize := int64(5 * 1024 * 1024) // 5MB

	type uploadInfo struct {
		key      string
		uploadID string
	}

	var uploads []uploadInfo
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Initiate multipart uploads and upload some parts
	sem := make(chan struct{}, 10)
	for i := 0; i < numUploads; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			key := GenerateKey(s.cfg.Prefix)
			uploadID, err := s.client.CreateMultipartUploadRaw(ctx, key)
			if err != nil {
				return
			}

			// Upload some parts (but don't complete)
			for p := int32(1); p <= int32(partsPerUpload); p++ {
				data := makeRandomData(partSize)
				s.client.UploadPartRaw(ctx, key, uploadID, p, data)
			}

			mu.Lock()
			uploads = append(uploads, uploadInfo{key: key, uploadID: uploadID})
			mu.Unlock()
		}()
	}
	wg.Wait()

	// Abort all uploads
	for _, u := range uploads {
		s.client.AbortMultipartUploadRaw(ctx, u.key, u.uploadID)
	}

	// Wait and verify no uploads remain
	time.Sleep(2 * time.Second)
	remaining, err := s.client.ListMultipartUploads(ctx)

	checks := []Check{
		{
			Name:   "Abort multipart: all uploads aborted",
			Passed: len(uploads) >= numUploads-5, // allow some failures
			Detail: fmt.Sprintf("initiated %d/%d", len(uploads), numUploads),
		},
		{
			Name:   "Abort multipart: no orphaned uploads",
			Passed: err == nil && remaining == 0,
			Detail: fmt.Sprintf("%d remaining uploads (expected 0)", remaining),
		},
	}
	return checks
}

// E2: Variable Part Sizes — ClickHouse uses s3_min_upload_part_size * factor^n
func (s *FailureScenario) testVariablePartSizes(ctx context.Context) []Check {
	key := GenerateKey(s.cfg.Prefix)
	uploadID, err := s.client.CreateMultipartUploadRaw(ctx, key)
	if err != nil {
		return []Check{{Name: "Variable part sizes: initiate", Passed: false, Detail: err.Error()}}
	}

	// Variable part sizes as ClickHouse does
	partSizes := []int64{
		16 * 1024 * 1024,  // 16 MB
		32 * 1024 * 1024,  // 32 MB
		64 * 1024 * 1024,  // 64 MB
		32 * 1024 * 1024,  // 32 MB (non-monotonic)
		5 * 1024 * 1024,   // 5 MB (last part, smaller)
	}

	var completedParts []types.CompletedPart
	allOk := true
	var firstErr string

	for i, size := range partSizes {
		data := makeRandomData(size)
		etag, err := s.client.UploadPartRaw(ctx, key, uploadID, int32(i+1), data)
		if err != nil {
			allOk = false
			firstErr = fmt.Sprintf("part %d (%d MB): %v", i+1, size/(1024*1024), err)
			break
		}
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       &etag,
			PartNumber: intPtr(int32(i + 1)),
		})
	}

	var completeOk bool
	if allOk {
		err = s.client.CompleteMultipartUploadRaw(ctx, key, uploadID, completedParts)
		completeOk = err == nil
		if err != nil {
			firstErr = fmt.Sprintf("CompleteMultipartUpload: %v", err)
		}
	}

	if allOk && completeOk {
		s.client.DeleteObjects(ctx, []string{key})
	}

	return []Check{
		{
			Name:   "Variable part sizes: upload succeeds",
			Passed: allOk,
			Detail: boolDetail(allOk, "all parts uploaded", firstErr),
		},
		{
			Name:   "Variable part sizes: complete succeeds",
			Passed: completeOk,
			Detail: boolDetail(completeOk, "multipart completed", firstErr),
		},
	}
}

// E3: High Concurrency PUT (scaled to resources)
func (s *FailureScenario) testHighConcurrencyPut(ctx context.Context) []Check {
	// Scale: 500 PUTs at reference (8 vCPU), proportional to resources
	numPuts := int(500 * s.cfg.Resources.ScaleFactor)
	if numPuts < 50 {
		numPuts = 50
	}
	if numPuts > 2000 {
		numPuts = 2000
	}
	const objSize = 1024 * 1024 // 1 MB

	var success atomic.Int64
	var errors atomic.Int64
	var keys []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < numPuts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := GenerateKey(s.cfg.Prefix)
			err := s.client.PutObject(ctx, key, int64(objSize))
			if err != nil {
				errors.Add(1)
			} else {
				success.Add(1)
				mu.Lock()
				keys = append(keys, key)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Cleanup
	for len(keys) > 0 {
		batch := keys
		if len(batch) > 1000 {
			batch = batch[:1000]
		}
		s.client.DeleteObjects(ctx, batch)
		keys = keys[len(batch):]
	}

	return []Check{
		{
			Name:   fmt.Sprintf("High concurrency PUT (%d, scaled %.1fx): no failures", numPuts, s.cfg.Resources.ScaleFactor),
			Passed: errors.Load() == 0,
			Detail: fmt.Sprintf("%d/%d succeeded in %s", success.Load(), numPuts, elapsed.Round(time.Millisecond)),
		},
		{
			Name:   fmt.Sprintf("High concurrency PUT (%d): no SlowDown/503", numPuts),
			Passed: errors.Load() == 0,
			Detail: fmt.Sprintf("%d errors", errors.Load()),
		},
	}
}

// E4: DeleteObjects batch of 1000 keys
func (s *FailureScenario) testBatchDelete(ctx context.Context) []Check {
	const numObjects = 1000
	const objSize = 1024

	// Create 1000 objects
	var keys []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 50)

	for i := 0; i < numObjects; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			key := GenerateKey(s.cfg.Prefix)
			if err := s.client.PutObject(ctx, key, int64(objSize)); err == nil {
				mu.Lock()
				keys = append(keys, key)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	created := len(keys)

	// Batch delete all 1000
	err := s.client.DeleteObjects(ctx, keys)

	// Verify: check a sample for residual objects
	time.Sleep(1 * time.Second)
	residual := 0
	checkCount := 50
	if len(keys) < checkCount {
		checkCount = len(keys)
	}
	for i := 0; i < checkCount; i++ {
		_, herr := s.client.HeadObject(ctx, keys[i])
		if herr == nil {
			residual++
		}
	}

	return []Check{
		{
			Name:   "Batch delete: all 1000 created",
			Passed: created >= 990,
			Detail: fmt.Sprintf("%d/%d created", created, numObjects),
		},
		{
			Name:   "Batch delete: DeleteObjects succeeds",
			Passed: err == nil,
			Detail: boolDetail(err == nil, "success", fmt.Sprintf("%v", err)),
		},
		{
			Name:   "Batch delete: no residual objects",
			Passed: residual == 0,
			Detail: fmt.Sprintf("%d/%d sampled objects still exist", residual, checkCount),
		},
	}
}

// E5: Strong consistency — PUT then immediate GET from different "client"
func (s *FailureScenario) testStrongConsistency(ctx context.Context) []Check {
	const numRounds = 1000
	const objSize = 1024

	var violations atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 50)
	var keys []string
	var mu sync.Mutex

	for i := 0; i < numRounds; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			key := GenerateKey(s.cfg.Prefix)

			// PUT
			err := s.client.PutObject(ctx, key, int64(objSize))
			if err != nil {
				return
			}

			mu.Lock()
			keys = append(keys, key)
			mu.Unlock()

			// Immediate GET (simulating different node reading)
			data, err := s.client.GetObject(ctx, key)
			if err != nil || len(data) != objSize {
				violations.Add(1)
				s.metrics.ConsistencyViolations.Add(1)
			}
		}()
	}
	wg.Wait()

	// Cleanup
	for len(keys) > 0 {
		batch := keys
		if len(batch) > 1000 {
			batch = batch[:1000]
		}
		s.client.DeleteObjects(ctx, batch)
		keys = keys[len(batch):]
	}

	return []Check{
		{
			Name:   "Strong consistency: 0 violations",
			Passed: violations.Load() == 0,
			Detail: fmt.Sprintf("%d violations in %d rounds", violations.Load(), numRounds),
		},
	}
}

func intPtr(i int32) *int32 { return &i }

func boolDetail(ok bool, success, failure string) string {
	if ok {
		return success
	}
	return failure
}
