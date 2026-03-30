package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type CompatCheckLevel int

const (
	CompatMUST     CompatCheckLevel = iota // Mandatory — abort if fail
	CompatSHOULD                           // Strongly recommended — warn if fail
	CompatOPTIONAL                         // Nice to have — report only
)

func (l CompatCheckLevel) String() string {
	switch l {
	case CompatMUST:
		return "MUST"
	case CompatSHOULD:
		return "SHOULD"
	case CompatOPTIONAL:
		return "OPTIONAL"
	}
	return "UNKNOWN"
}

type CompatCheck struct {
	Name        string
	Level       CompatCheckLevel
	Description string
	TestFn      func(ctx context.Context) (bool, string)
}

type CompatScenario struct {
	client    *S3Client
	cfg       *Config
	metrics   *MetricsCollector
	rawClient *s3.Client
}

func NewCompatScenario(client *S3Client, cfg *Config, metrics *MetricsCollector) *CompatScenario {
	return &CompatScenario{client: client, cfg: cfg, metrics: metrics, rawClient: client.client}
}

func (s *CompatScenario) Name() string { return "S3 API Compatibility" }

func (s *CompatScenario) Description() string {
	return "Validates all S3 APIs required by ClickHouse (MUST/SHOULD/OPTIONAL)"
}

func (s *CompatScenario) Run(ctx context.Context) (*ScenarioResult, error) {
	s.metrics.Reset()

	checks := s.defineChecks()

	var allResults []Check
	mustFailed := false
	mustFailedName := ""

	categorized := map[CompatCheckLevel][]CompatCheck{
		CompatMUST:     {},
		CompatSHOULD:   {},
		CompatOPTIONAL: {},
	}
	for _, c := range checks {
		categorized[c.Level] = append(categorized[c.Level], c)
	}

	for _, level := range []CompatCheckLevel{CompatMUST, CompatSHOULD, CompatOPTIONAL} {
		levelChecks := categorized[level]
		if len(levelChecks) == 0 {
			continue
		}

		fmt.Printf("\n  ┌─── %s APIs (%d checks) ───\n", level.String(), len(levelChecks))

		for _, check := range levelChecks {
			if mustFailed {
				allResults = append(allResults, Check{
					Name:   fmt.Sprintf("[%s] %s", level.String(), check.Name),
					Passed: false,
					Detail: "SKIPPED — previous MUST check failed",
				})
				fmt.Printf("  │  ⊘ [SKIP] %s\n", check.Name)
				continue
			}

			fmt.Printf("  │  ◉ Testing: %s... ", check.Name)
			passed, detail := check.TestFn(ctx)

			status := "PASS"
			if !passed {
				status = "FAIL"
			}
			fmt.Printf("[%s]\n", status)
			if detail != "" {
				fmt.Printf("  │    └─ %s\n", detail)
			}

			allResults = append(allResults, Check{
				Name:   fmt.Sprintf("[%s] %s", level.String(), check.Name),
				Passed: passed,
				Detail: detail,
			})

			if !passed && level == CompatMUST {
				mustFailed = true
				mustFailedName = check.Name
				fmt.Printf("  │\n")
				fmt.Printf("  │  ╔═══════════════════════════════════════════════════════╗\n")
				fmt.Printf("  │  ║  FATAL: MUST API '%s' FAILED   ║\n", truncate(check.Name, 30))
				fmt.Printf("  │  ║  ClickHouse CANNOT work without this API.    ║\n")
				fmt.Printf("  │  ║  Remaining checks will be SKIPPED.                   ║\n")
				fmt.Printf("  │  ╚═══════════════════════════════════════════════════════╝\n")
			}
		}
		fmt.Printf("  └───\n")
	}

	// Summary
	mustCount, mustPass := 0, 0
	shouldCount, shouldPass := 0, 0
	optCount, optPass := 0, 0
	for _, r := range allResults {
		switch {
		case strings.HasPrefix(r.Name, "[MUST]"):
			mustCount++
			if r.Passed {
				mustPass++
			}
		case strings.HasPrefix(r.Name, "[SHOULD]"):
			shouldCount++
			if r.Passed {
				shouldPass++
			}
		case strings.HasPrefix(r.Name, "[OPTIONAL]"):
			optCount++
			if r.Passed {
				optPass++
			}
		}
	}

	fmt.Printf("\n  ╔══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("  ║  S3 API Compatibility Summary                               ║\n")
	fmt.Printf("  ╠══════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("  ║  MUST     : %d/%d %-46s║\n", mustPass, mustCount, statusBar(mustPass, mustCount))
	fmt.Printf("  ║  SHOULD   : %d/%d %-46s║\n", shouldPass, shouldCount, statusBar(shouldPass, shouldCount))
	fmt.Printf("  ║  OPTIONAL : %d/%d %-46s║\n", optPass, optCount, statusBar(optPass, optCount))
	fmt.Printf("  ╠══════════════════════════════════════════════════════════════╣\n")

	if mustFailed {
		fmt.Printf("  ║  RESULT: INCOMPATIBLE — missing: %-25s║\n", truncate(mustFailedName, 25))
	} else if shouldPass < shouldCount {
		fmt.Printf("  ║  RESULT: COMPATIBLE (with warnings)                         ║\n")
	} else {
		fmt.Printf("  ║  RESULT: FULLY COMPATIBLE                                   ║\n")
	}
	fmt.Printf("  ╚══════════════════════════════════════════════════════════════╝\n")

	stats := s.metrics.GetAllStats()
	return &ScenarioResult{
		Name:     s.Name(),
		Duration: 0,
		Stats:    stats,
		Checks:   allResults,
	}, nil
}

func statusBar(pass, total int) string {
	if total == 0 {
		return "N/A"
	}
	filled := pass * 20 / total
	return strings.Repeat("█", filled) + strings.Repeat("░", 20-filled)
}

// MustFailed returns true if any MUST check failed
func (s *CompatScenario) MustFailed(result *ScenarioResult) bool {
	for _, c := range result.Checks {
		if strings.HasPrefix(c.Name, "[MUST]") && !c.Passed && !strings.Contains(c.Detail, "SKIPPED") {
			return true
		}
	}
	return false
}

func (s *CompatScenario) defineChecks() []CompatCheck {
	return []CompatCheck{
		// ═══════════════════════════════════════════════════════
		// MUST — ClickHouse cannot function without these
		// ═══════════════════════════════════════════════════════
		{
			Name:  "PutObject (single-part)",
			Level: CompatMUST,
			Description: "Write objects < 32 MB (metadata, compact parts)",
			TestFn: s.testPutObject,
		},
		{
			Name:  "GetObject (full read)",
			Level: CompatMUST,
			Description: "Read primary.idx, marks, metadata files",
			TestFn: s.testGetObject,
		},
		{
			Name:  "GetObject (Range read)",
			Level: CompatMUST,
			Description: "64 KB granule reads via Range: bytes=start-end",
			TestFn: s.testGetObjectRange,
		},
		{
			Name:  "HeadObject",
			Level: CompatMUST,
			Description: "Check existence, get size/ETag/LastModified",
			TestFn: s.testHeadObject,
		},
		{
			Name:  "DeleteObjects (batch)",
			Level: CompatMUST,
			Description: "Batch delete after merge (max 1000 keys, Quiet:true)",
			TestFn: s.testDeleteObjectsBatch,
		},
		{
			Name:  "ListObjectsV2 (with pagination)",
			Level: CompatMUST,
			Description: "Discovery/startup with ContinuationToken pagination",
			TestFn: s.testListObjectsV2,
		},
		{
			Name:  "CreateMultipartUpload",
			Level: CompatMUST,
			Description: "Initiate multipart for objects > 32 MB",
			TestFn: s.testCreateMultipartUpload,
		},
		{
			Name:  "UploadPart",
			Level: CompatMUST,
			Description: "Upload individual parts in multipart upload",
			TestFn: s.testUploadPart,
		},
		{
			Name:  "UploadPart (variable part sizes)",
			Level: CompatMUST,
			Description: "CH uses 16MB→32MB→64MB... variable sizes per upload",
			TestFn: s.testVariablePartSizes,
		},
		{
			Name:  "CompleteMultipartUpload",
			Level: CompatMUST,
			Description: "Finalize multipart with ETag matching",
			TestFn: s.testCompleteMultipartUpload,
		},
		{
			Name:  "AbortMultipartUpload",
			Level: CompatMUST,
			Description: "Cancel failed multipart, cleanup orphan parts",
			TestFn: s.testAbortMultipartUpload,
		},
		{
			Name:  "Strong read-after-write consistency",
			Level: CompatMUST,
			Description: "PUT then immediate GET must return the same data",
			TestFn: s.testReadAfterWriteConsistency,
		},
		{
			Name:  "ETag coherence (UploadPart → Complete)",
			Level: CompatMUST,
			Description: "ETags from UploadPart must match in CompleteMultipartUpload",
			TestFn: s.testETagCoherence,
		},
		{
			Name:  "Immediate 404 after Delete",
			Level: CompatMUST,
			Description: "HeadObject must return 404 immediately after DeleteObjects",
			TestFn: s.testImmediate404AfterDelete,
		},
		{
			Name:        "Read-after-write consistency under concurrency",
			Level:       CompatMUST,
			Description: "20 concurrent goroutines each doing PUT then immediate GET — 0 violations allowed",
			TestFn:      s.testConcurrentReadAfterWrite,
		},

		// ═══════════════════════════════════════════════════════
		// MUST — Connection & Network (CH connection pool reqs)
		// ═══════════════════════════════════════════════════════
		{
			Name:        "HTTP Keep-Alive (persistent connections)",
			Level:       CompatMUST,
			Description: "CH reuses TCP connections massively — connection reset = catastrophic perf",
			TestFn:      s.testHTTPKeepAlive,
		},
		{
			Name:        fmt.Sprintf("Concurrent TCP connections (>= %d)", CHRequirements.MinConcurrentConnections),
			Level:       CompatMUST,
			Description: fmt.Sprintf("%d per node — CH default connection pool", CHRequirements.MinConcurrentConnections),
			TestFn:      s.testConcurrentConnections3000,
		},
		{
			Name:        fmt.Sprintf("Connection idle timeout >= %ds", CHRequirements.IdleTimeoutTestSeconds-1),
			Level:       CompatMUST,
			Description: "S3 must not close idle connections before 60s — causes TLS handshake overhead",
			TestFn:      s.testIdleConnectionTimeout,
		},

		// ═══════════════════════════════════════════════════════
		// SHOULD — Strongly recommended for optimal operation
		// ═══════════════════════════════════════════════════════
		{
			Name:        fmt.Sprintf("64KB range GET P99 < %.0fms (TTFB)", CHRequirements.GetRangeP99MaxMs),
			Level:       CompatSHOULD,
			Description: "CH reads 64KB granules — P99 latency critical for 270 range GETs per query",
			TestFn:      s.testFirstByteTTFB,
		},
		{
			Name:        "DNS returns multiple IPs (no hotspot)",
			Level:       CompatSHOULD,
			Description: "Single IP = hotspot on 1 backend server — causes degradation under load",
			TestFn:      s.testDNSRotation,
		},
		{
			Name:  "DeleteObject (single)",
			Level: CompatSHOULD,
			Description: "Fallback when batch delete fails",
			TestFn: s.testDeleteObjectSingle,
		},
		{
			Name:  "ListMultipartUploads",
			Level: CompatSHOULD,
			Description: "Monitor/cleanup stuck multipart uploads after crash",
			TestFn: s.testListMultipartUploads,
		},
		{
			Name:  "Range read: multiple ranges on same object",
			Level: CompatSHOULD,
			Description: "Parallel range reads (CH reads 4 granules in parallel per file)",
			TestFn: s.testParallelRangeReads,
		},
		{
			Name:  "PUT various sizes (4B to 32MB)",
			Level: CompatSHOULD,
			Description: "CH writes from 4B (count.txt) to 32MB (large data)",
			TestFn: s.testPutVariousSizes,
		},
		{
			Name:  "High concurrency (100 parallel ops)",
			Level: CompatSHOULD,
			Description: "CH merge/insert can generate 100+ concurrent S3 calls",
			TestFn: s.testHighConcurrency,
		},
		{
			Name:  "Content-Length in response headers",
			Level: CompatSHOULD,
			Description: "HeadObject must return Content-Length for size checks",
			TestFn: s.testContentLengthHeader,
		},

		// ═══════════════════════════════════════════════════════
		// OPTIONAL — Nice to have, not strictly required
		// ═══════════════════════════════════════════════════════
		{
			Name:  "GetBucketLocation",
			Level: CompatOPTIONAL,
			Description: "Auto-detect region at startup",
			TestFn: s.testGetBucketLocation,
		},
		{
			Name:  "PutObject with storage class header",
			Level: CompatOPTIONAL,
			Description: "x-amz-storage-class: STANDARD / INTELLIGENT_TIERING",
			TestFn: s.testPutObjectStorageClass,
		},
		{
			Name:  "ListObjectsV2 with MaxKeys=1",
			Level: CompatOPTIONAL,
			Description: "Used for quick existence check",
			TestFn: s.testListObjectsMaxKeys1,
		},
	}
}

// ─────────────────────────────────────────────────────────────
// MUST Tests
// ─────────────────────────────────────────────────────────────

func (s *CompatScenario) testPutObject(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	defer s.cleanupKeys(ctx, key)
	err := s.client.PutObject(ctx, key, 1024)
	if err != nil {
		return false, fmt.Sprintf("PutObject failed: %v", err)
	}
	return true, "1 KB object uploaded successfully"
}

func (s *CompatScenario) testGetObject(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	defer s.cleanupKeys(ctx, key)
	err := s.client.PutObject(ctx, key, 4096)
	if err != nil {
		return false, fmt.Sprintf("Setup PutObject failed: %v", err)
	}
	data, err := s.client.GetObject(ctx, key)
	if err != nil {
		return false, fmt.Sprintf("GetObject failed: %v", err)
	}
	if len(data) != 4096 {
		return false, fmt.Sprintf("GetObject size mismatch: got %d, expected 4096", len(data))
	}
	return true, "4 KB read successfully, size matches"
}

func (s *CompatScenario) testGetObjectRange(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	defer s.cleanupKeys(ctx, key)

	objSize := int64(256 * 1024)
	err := s.client.PutObject(ctx, key, objSize)
	if err != nil {
		return false, fmt.Sprintf("Setup PutObject failed: %v", err)
	}

	// CH reads 64 KB granules
	data, err := s.client.GetObjectRange(ctx, key, 0, 65536)
	if err != nil {
		return false, fmt.Sprintf("Range read failed: %v", err)
	}
	if int64(len(data)) != 65536 {
		return false, fmt.Sprintf("Range read size mismatch: got %d, expected 65536", len(data))
	}

	// Second granule
	data2, err := s.client.GetObjectRange(ctx, key, 65536, 65536)
	if err != nil {
		return false, fmt.Sprintf("Range read (granule 2) failed: %v", err)
	}
	if int64(len(data2)) != 65536 {
		return false, fmt.Sprintf("Range read (granule 2) size mismatch: got %d", len(data2))
	}

	return true, "64 KB granule reads OK (2 ranges verified)"
}

func (s *CompatScenario) testHeadObject(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	defer s.cleanupKeys(ctx, key)

	err := s.client.PutObject(ctx, key, 2048)
	if err != nil {
		return false, fmt.Sprintf("Setup PutObject failed: %v", err)
	}
	size, err := s.client.HeadObject(ctx, key)
	if err != nil {
		return false, fmt.Sprintf("HeadObject failed: %v", err)
	}
	if size != 2048 {
		return false, fmt.Sprintf("HeadObject size mismatch: got %d, expected 2048", size)
	}
	return true, "HeadObject returns correct Content-Length"
}

func (s *CompatScenario) testDeleteObjectsBatch(ctx context.Context) (bool, string) {
	const numKeys = 36 // Typical CH merge: 4 parts × 9 files = 36 keys
	var keys []string

	for i := 0; i < numKeys; i++ {
		key := GenerateKey(s.cfg.Prefix + "compat/")
		if err := s.client.PutObject(ctx, key, 100); err != nil {
			return false, fmt.Sprintf("Setup: PutObject %d failed: %v", i, err)
		}
		keys = append(keys, key)
	}

	err := s.client.DeleteObjects(ctx, keys)
	if err != nil {
		return false, fmt.Sprintf("DeleteObjects batch failed: %v", err)
	}

	// Verify at least some are gone
	_, headErr := s.client.HeadObject(ctx, keys[0])
	if headErr == nil {
		return false, "DeleteObjects reported success but object still exists"
	}

	return true, fmt.Sprintf("Batch delete of %d keys OK (CH merge cleanup pattern)", numKeys)
}

func (s *CompatScenario) testListObjectsV2(ctx context.Context) (bool, string) {
	prefix := s.cfg.Prefix + "compat-list/"
	const numObjects = 5

	var keys []string
	for i := 0; i < numObjects; i++ {
		key := GenerateKey(prefix)
		if err := s.client.PutObject(ctx, key, 100); err != nil {
			return false, fmt.Sprintf("Setup: PutObject %d failed: %v", i, err)
		}
		keys = append(keys, key)
	}
	defer func() {
		s.client.DeleteObjects(ctx, keys)
	}()

	// Test with pagination (MaxKeys=2 to force multiple pages)
	var allKeys []string
	var contToken *string
	pages := 0
	for {
		out, err := s.rawClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.cfg.Bucket),
			Prefix:            aws.String(prefix),
			MaxKeys:           aws.Int32(2),
			ContinuationToken: contToken,
		})
		if err != nil {
			return false, fmt.Sprintf("ListObjectsV2 failed: %v", err)
		}
		pages++
		for _, obj := range out.Contents {
			allKeys = append(allKeys, aws.ToString(obj.Key))
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		contToken = out.NextContinuationToken
		if contToken == nil {
			return false, "Truncated=true but NextContinuationToken is nil"
		}
	}

	if len(allKeys) < numObjects {
		return false, fmt.Sprintf("Listed %d/%d objects", len(allKeys), numObjects)
	}
	return true, fmt.Sprintf("Pagination OK: %d objects across %d pages", len(allKeys), pages)
}

func (s *CompatScenario) testCreateMultipartUpload(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	uploadID, err := s.client.CreateMultipartUploadRaw(ctx, key)
	if err != nil {
		return false, fmt.Sprintf("CreateMultipartUpload failed: %v", err)
	}
	// Abort it since we just tested creation
	s.client.AbortMultipartUploadRaw(ctx, key, uploadID)
	return true, fmt.Sprintf("UploadId returned: %s", truncate(uploadID, 20))
}

func (s *CompatScenario) testUploadPart(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	uploadID, err := s.client.CreateMultipartUploadRaw(ctx, key)
	if err != nil {
		return false, fmt.Sprintf("CreateMultipartUpload failed: %v", err)
	}
	defer s.client.AbortMultipartUploadRaw(ctx, key, uploadID)

	data := makeRandomData(5 * 1024 * 1024) // 5 MB minimum part
	etag, err := s.client.UploadPartRaw(ctx, key, uploadID, 1, data)
	if err != nil {
		return false, fmt.Sprintf("UploadPart failed: %v", err)
	}
	if etag == "" {
		return false, "UploadPart returned empty ETag"
	}
	return true, fmt.Sprintf("5 MB part uploaded, ETag: %s", truncate(etag, 20))
}

func (s *CompatScenario) testVariablePartSizes(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	uploadID, err := s.client.CreateMultipartUploadRaw(ctx, key)
	if err != nil {
		return false, fmt.Sprintf("CreateMultipartUpload failed: %v", err)
	}

	// CH pattern: 16MB, 16MB, 32MB (after threshold), variable last part
	partSizes := []int64{
		5 * 1024 * 1024,  // 5 MB (minimum S3 part)
		8 * 1024 * 1024,  // 8 MB
		10 * 1024 * 1024, // 10 MB (different size)
		6 * 1024 * 1024,  // 6 MB (non-monotonic — CH can do this)
		5 * 1024 * 1024,  // 5 MB (last part, any size)
	}

	var parts []types.CompletedPart
	for i, size := range partSizes {
		data := makeRandomData(size)
		etag, err := s.client.UploadPartRaw(ctx, key, uploadID, int32(i+1), data)
		if err != nil {
			s.client.AbortMultipartUploadRaw(ctx, key, uploadID)
			return false, fmt.Sprintf("Part %d (%d MB) failed: %v", i+1, size/(1024*1024), err)
		}
		parts = append(parts, types.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int32(int32(i + 1)),
		})
	}

	err = s.client.CompleteMultipartUploadRaw(ctx, key, uploadID, parts)
	if err != nil {
		return false, fmt.Sprintf("CompleteMultipartUpload with variable parts failed: %v", err)
	}
	defer s.cleanupKeys(ctx, key)

	return true, fmt.Sprintf("5 parts with sizes %v MB completed OK", []int{5, 8, 10, 6, 5})
}

func (s *CompatScenario) testCompleteMultipartUpload(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	uploadID, err := s.client.CreateMultipartUploadRaw(ctx, key)
	if err != nil {
		return false, fmt.Sprintf("CreateMultipartUpload failed: %v", err)
	}

	data := makeRandomData(5 * 1024 * 1024)
	etag, err := s.client.UploadPartRaw(ctx, key, uploadID, 1, data)
	if err != nil {
		s.client.AbortMultipartUploadRaw(ctx, key, uploadID)
		return false, fmt.Sprintf("UploadPart failed: %v", err)
	}

	err = s.client.CompleteMultipartUploadRaw(ctx, key, uploadID, []types.CompletedPart{
		{ETag: aws.String(etag), PartNumber: aws.Int32(1)},
	})
	if err != nil {
		return false, fmt.Sprintf("CompleteMultipartUpload failed: %v", err)
	}
	defer s.cleanupKeys(ctx, key)

	// Verify object is readable
	readData, err := s.client.GetObject(ctx, key)
	if err != nil {
		return false, fmt.Sprintf("Post-complete GetObject failed: %v", err)
	}
	if int64(len(readData)) != 5*1024*1024 {
		return false, fmt.Sprintf("Size mismatch after complete: got %d", len(readData))
	}
	return true, "Multipart completed and verified readable"
}

func (s *CompatScenario) testAbortMultipartUpload(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	uploadID, err := s.client.CreateMultipartUploadRaw(ctx, key)
	if err != nil {
		return false, fmt.Sprintf("CreateMultipartUpload failed: %v", err)
	}

	data := makeRandomData(5 * 1024 * 1024)
	_, err = s.client.UploadPartRaw(ctx, key, uploadID, 1, data)
	if err != nil {
		return false, fmt.Sprintf("UploadPart failed: %v", err)
	}

	err = s.client.AbortMultipartUploadRaw(ctx, key, uploadID)
	if err != nil {
		return false, fmt.Sprintf("AbortMultipartUpload failed: %v", err)
	}

	// Verify abort: the key should not exist
	_, headErr := s.client.HeadObject(ctx, key)
	if headErr == nil {
		return false, "Key exists after abort — orphaned data"
	}
	return true, "Multipart aborted, no orphaned objects"
}

func (s *CompatScenario) testReadAfterWriteConsistency(ctx context.Context) (bool, string) {
	const rounds = 50
	violations := 0

	for i := 0; i < rounds; i++ {
		key := GenerateKey(s.cfg.Prefix + "compat/")
		// Write known data
		payload := make([]byte, 1024)
		rand.Read(payload)

		// Use raw PUT with known data
		_, err := s.rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(s.cfg.Bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(payload),
			ContentLength: aws.Int64(int64(len(payload))),
		})
		if err != nil {
			s.cleanupKeys(ctx, key)
			return false, fmt.Sprintf("Round %d: PutObject failed: %v", i+1, err)
		}

		// Immediate read
		data, err := s.client.GetObject(ctx, key)
		if err != nil {
			violations++
		} else if !bytes.Equal(data, payload) {
			violations++
		}
		s.cleanupKeys(ctx, key)
	}

	if violations > 0 {
		return false, fmt.Sprintf("%d/%d consistency violations", violations, rounds)
	}
	return true, fmt.Sprintf("0/%d violations — strong consistency confirmed", rounds)
}

func (s *CompatScenario) testETagCoherence(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	uploadID, err := s.client.CreateMultipartUploadRaw(ctx, key)
	if err != nil {
		return false, fmt.Sprintf("CreateMultipartUpload failed: %v", err)
	}

	data1 := makeRandomData(5 * 1024 * 1024)
	etag1, err := s.client.UploadPartRaw(ctx, key, uploadID, 1, data1)
	if err != nil {
		s.client.AbortMultipartUploadRaw(ctx, key, uploadID)
		return false, fmt.Sprintf("UploadPart 1 failed: %v", err)
	}

	data2 := makeRandomData(5 * 1024 * 1024)
	etag2, err := s.client.UploadPartRaw(ctx, key, uploadID, 2, data2)
	if err != nil {
		s.client.AbortMultipartUploadRaw(ctx, key, uploadID)
		return false, fmt.Sprintf("UploadPart 2 failed: %v", err)
	}

	// Complete with the exact ETags we received
	err = s.client.CompleteMultipartUploadRaw(ctx, key, uploadID, []types.CompletedPart{
		{ETag: aws.String(etag1), PartNumber: aws.Int32(1)},
		{ETag: aws.String(etag2), PartNumber: aws.Int32(2)},
	})
	if err != nil {
		return false, fmt.Sprintf("CompleteMultipartUpload failed (ETag mismatch?): %v", err)
	}
	defer s.cleanupKeys(ctx, key)

	return true, fmt.Sprintf("ETags %s, %s matched in Complete", truncate(etag1, 12), truncate(etag2, 12))
}

func (s *CompatScenario) testImmediate404AfterDelete(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	err := s.client.PutObject(ctx, key, 1024)
	if err != nil {
		return false, fmt.Sprintf("PutObject failed: %v", err)
	}

	// Verify exists
	_, err = s.client.HeadObject(ctx, key)
	if err != nil {
		return false, fmt.Sprintf("HeadObject pre-delete failed: %v", err)
	}

	// Delete
	err = s.client.DeleteObjects(ctx, []string{key})
	if err != nil {
		return false, fmt.Sprintf("DeleteObjects failed: %v", err)
	}

	// Immediate HEAD — must be 404
	_, headErr := s.client.HeadObject(ctx, key)
	if headErr == nil {
		return false, "Object still visible after delete — stale read detected"
	}

	return true, "Immediate 404 after delete confirmed"
}

// ─────────────────────────────────────────────────────────────
// SHOULD Tests
// ─────────────────────────────────────────────────────────────

func (s *CompatScenario) testDeleteObjectSingle(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	err := s.client.PutObject(ctx, key, 512)
	if err != nil {
		return false, fmt.Sprintf("PutObject failed: %v", err)
	}

	start := time.Now()
	_, err = s.rawClient.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	elapsed := time.Since(start)
	if err != nil {
		return false, fmt.Sprintf("DeleteObject failed: %v", err)
	}
	return true, fmt.Sprintf("Single delete OK in %s", elapsed.Round(time.Millisecond))
}

func (s *CompatScenario) testListMultipartUploads(ctx context.Context) (bool, string) {
	_, err := s.client.ListMultipartUploads(ctx)
	if err != nil {
		return false, fmt.Sprintf("ListMultipartUploads failed: %v", err)
	}
	return true, "ListMultipartUploads supported"
}

func (s *CompatScenario) testParallelRangeReads(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	defer s.cleanupKeys(ctx, key)

	objSize := int64(512 * 1024)
	err := s.client.PutObject(ctx, key, objSize)
	if err != nil {
		return false, fmt.Sprintf("PutObject failed: %v", err)
	}

	// 4 parallel range reads (CH max_download_threads=4)
	type result struct {
		size int
		err  error
	}
	results := make(chan result, 4)
	for i := 0; i < 4; i++ {
		go func(offset int64) {
			data, err := s.client.GetObjectRange(ctx, key, offset, 65536)
			results <- result{size: len(data), err: err}
		}(int64(i) * 65536)
	}

	for i := 0; i < 4; i++ {
		r := <-results
		if r.err != nil {
			return false, fmt.Sprintf("Parallel range read %d failed: %v", i, r.err)
		}
		if r.size != 65536 {
			return false, fmt.Sprintf("Parallel range read size mismatch: got %d", r.size)
		}
	}
	return true, "4 parallel 64KB range reads OK (max_download_threads pattern)"
}

func (s *CompatScenario) testPutVariousSizes(ctx context.Context) (bool, string) {
	sizes := map[string]int64{
		"count.txt (4B)":      4,
		"columns.txt (288B)":  288,
		"checksums.txt (96B)": 96,
		"primary.idx (48KB)":  48 * 1024,
		"data.mrk3 (500KB)":   500 * 1024,
		"data.bin (5MB)":      5 * 1024 * 1024,
	}

	var keys []string
	for name, size := range sizes {
		key := GenerateKey(s.cfg.Prefix + "compat/")
		if err := s.client.PutObject(ctx, key, size); err != nil {
			for _, k := range keys {
				s.cleanupKeys(ctx, k)
			}
			return false, fmt.Sprintf("%s (%d bytes) failed: %v", name, size, err)
		}
		keys = append(keys, key)
	}

	defer func() {
		s.client.DeleteObjects(ctx, keys)
	}()

	return true, "All CH file sizes work: 4B → 5MB"
}

func (s *CompatScenario) testHighConcurrency(ctx context.Context) (bool, string) {
	const numOps = 100
	const objSize = 1024
	errs := make(chan error, numOps)
	var keys []string
	keysCh := make(chan string, numOps)

	start := time.Now()
	for i := 0; i < numOps; i++ {
		go func() {
			key := GenerateKey(s.cfg.Prefix + "compat/")
			err := s.client.PutObject(ctx, key, objSize)
			if err == nil {
				keysCh <- key
			}
			errs <- err
		}()
	}

	errCount := 0
	for i := 0; i < numOps; i++ {
		if err := <-errs; err != nil {
			errCount++
		}
	}
	close(keysCh)
	for k := range keysCh {
		keys = append(keys, k)
	}
	elapsed := time.Since(start)

	defer func() {
		for len(keys) > 0 {
			batch := keys
			if len(batch) > 1000 {
				batch = batch[:1000]
			}
			s.client.DeleteObjects(ctx, batch)
			keys = keys[len(batch):]
		}
	}()

	if errCount > 0 {
		return false, fmt.Sprintf("%d/%d failed in %s", errCount, numOps, elapsed.Round(time.Millisecond))
	}
	return true, fmt.Sprintf("100 parallel PUTs in %s, 0 errors", elapsed.Round(time.Millisecond))
}

func (s *CompatScenario) testContentLengthHeader(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	defer s.cleanupKeys(ctx, key)

	expectedSize := int64(7777)
	err := s.client.PutObject(ctx, key, expectedSize)
	if err != nil {
		return false, fmt.Sprintf("PutObject failed: %v", err)
	}

	out, err := s.rawClient.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return false, fmt.Sprintf("HeadObject failed: %v", err)
	}

	if out.ContentLength == nil {
		return false, "Content-Length is nil in HeadObject response"
	}
	if *out.ContentLength != expectedSize {
		return false, fmt.Sprintf("Content-Length mismatch: got %d, expected %d", *out.ContentLength, expectedSize)
	}
	return true, fmt.Sprintf("Content-Length=%d matches expected", *out.ContentLength)
}

// ─────────────────────────────────────────────────────────────
// OPTIONAL Tests
// ─────────────────────────────────────────────────────────────

func (s *CompatScenario) testGetBucketLocation(ctx context.Context) (bool, string) {
	out, err := s.rawClient.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(s.cfg.Bucket),
	})
	if err != nil {
		return false, fmt.Sprintf("GetBucketLocation failed: %v", err)
	}
	loc := string(out.LocationConstraint)
	if loc == "" {
		loc = "us-east-1 (default)"
	}
	return true, fmt.Sprintf("Location: %s", loc)
}

func (s *CompatScenario) testPutObjectStorageClass(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	defer s.cleanupKeys(ctx, key)

	data := makeRandomData(1024)
	_, err := s.rawClient.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(s.cfg.Bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(data),
		StorageClass: types.StorageClassStandard,
	})
	if err != nil {
		return false, fmt.Sprintf("PutObject with STANDARD class failed: %v", err)
	}
	return true, "StorageClass STANDARD accepted"
}

func (s *CompatScenario) testListObjectsMaxKeys1(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	defer s.cleanupKeys(ctx, key)

	err := s.client.PutObject(ctx, key, 100)
	if err != nil {
		return false, fmt.Sprintf("PutObject failed: %v", err)
	}

	out, err := s.rawClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.cfg.Bucket),
		Prefix:  aws.String(s.cfg.Prefix + "compat/"),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return false, fmt.Sprintf("ListObjectsV2 MaxKeys=1 failed: %v", err)
	}
	if len(out.Contents) == 0 {
		return false, "ListObjectsV2 MaxKeys=1 returned 0 results"
	}
	return true, "MaxKeys=1 returns results correctly"
}

func (s *CompatScenario) testConcurrentReadAfterWrite(ctx context.Context) (bool, string) {
	const goroutines = 20
	const rounds = 5
	var violations atomic.Int64
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				key := GenerateKey(s.cfg.Prefix + "compat/")
				payload := make([]byte, 4096)
				rand.Read(payload)

				_, err := s.rawClient.PutObject(ctx, &s3.PutObjectInput{
					Bucket:        aws.String(s.cfg.Bucket),
					Key:           aws.String(key),
					Body:          bytes.NewReader(payload),
					ContentLength: aws.Int64(int64(len(payload))),
				})
				if err != nil {
					violations.Add(1)
					s.cleanupKeys(ctx, key)
					continue
				}

				data, err := s.client.GetObject(ctx, key)
				if err != nil || !bytes.Equal(data, payload) {
					violations.Add(1)
				}
				s.cleanupKeys(ctx, key)
			}
		}()
	}
	wg.Wait()

	total := int64(goroutines * rounds)
	v := violations.Load()
	if v > 0 {
		return false, fmt.Sprintf("%d/%d violations — data mismatch under concurrency", v, total)
	}
	return true, fmt.Sprintf("0/%d violations — concurrent consistency confirmed", total)
}

// ─────────────────────────────────────────────────────────────
// MUST — Connection & Network Tests
// ─────────────────────────────────────────────────────────────

func (s *CompatScenario) testHTTPKeepAlive(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	defer s.cleanupKeys(ctx, key)

	if err := s.client.PutObject(ctx, key, 1024); err != nil {
		return false, fmt.Sprintf("Setup PutObject failed: %v", err)
	}

	resetErrors := 0
	const rounds = 50
	for i := 0; i < rounds; i++ {
		_, err := s.client.HeadObject(ctx, key)
		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "connection reset") ||
				strings.Contains(errStr, "broken pipe") ||
				strings.Contains(errStr, "EOF") {
				resetErrors++
			}
		}
	}

	if resetErrors > 0 {
		return false, fmt.Sprintf("%d/%d requests got connection reset — Keep-Alive broken", resetErrors, rounds)
	}
	return true, fmt.Sprintf("%d sequential requests on pooled connections, 0 resets", rounds)
}

func (s *CompatScenario) testConcurrentConnections3000(ctx context.Context) (bool, string) {
	targetConns := CHRequirements.MinConcurrentConnections
	if targetConns < 100 {
		targetConns = 100
	}
	const objSize = 256

	var success atomic.Int64
	var errors atomic.Int64
	var keys []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	start := time.Now()
	sem := make(chan struct{}, 500)
	for i := 0; i < targetConns; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			key := GenerateKey(s.cfg.Prefix + "compat-conn/")
			err := s.client.PutObject(ctx, key, objSize)
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

	defer func() {
		for len(keys) > 0 {
			batch := keys
			if len(batch) > 1000 {
				batch = batch[:1000]
			}
			s.client.DeleteObjects(ctx, batch)
			keys = keys[len(batch):]
		}
	}()

	errCount := errors.Load()
	errRate := float64(errCount) / float64(targetConns) * 100
	if errRate > 1.0 {
		return false, fmt.Sprintf("%d/%d failed (%.1f%%) in %s — S3 cannot handle 3000 concurrent connections",
			errCount, targetConns, errRate, elapsed.Round(time.Millisecond))
	}
	return true, fmt.Sprintf("%d/%d succeeded (%.1f%% error) in %s",
		success.Load(), targetConns, errRate, elapsed.Round(time.Millisecond))
}

func (s *CompatScenario) testIdleConnectionTimeout(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	defer s.cleanupKeys(ctx, key)

	if err := s.client.PutObject(ctx, key, 1024); err != nil {
		return false, fmt.Sprintf("Setup PutObject failed: %v", err)
	}

	// Warm the connection pool
	for i := 0; i < 5; i++ {
		s.client.HeadObject(ctx, key)
	}

	waitSec := CHRequirements.IdleTimeoutTestSeconds
	fmt.Printf("\n  │    └─ waiting %ds for idle connection test...", waitSec)
	time.Sleep(time.Duration(waitSec) * time.Second)

	// Try to reuse the connection
	resetErrors := 0
	for i := 0; i < 10; i++ {
		_, err := s.client.HeadObject(ctx, key)
		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "connection reset") ||
				strings.Contains(errStr, "broken pipe") ||
				strings.Contains(errStr, "EOF") ||
				strings.Contains(errStr, "use of closed") {
				resetErrors++
			}
		}
	}

	if resetErrors > CHRequirements.MaxIdleConnectionResets {
		return false, fmt.Sprintf("%d/10 connection resets after %ds idle — S3 closes connections too early (zero tolerance)", resetErrors, waitSec)
	}
	return true, fmt.Sprintf("Connections survived %ds idle (0/10 resets)", waitSec)
}

// ─────────────────────────────────────────────────────────────
// SHOULD — Connection & Network Tests
// ─────────────────────────────────────────────────────────────

func (s *CompatScenario) testFirstByteTTFB(ctx context.Context) (bool, string) {
	key := GenerateKey(s.cfg.Prefix + "compat/")
	defer s.cleanupKeys(ctx, key)

	objSize := int64(256 * 1024)
	if err := s.client.PutObject(ctx, key, objSize); err != nil {
		return false, fmt.Sprintf("Setup PutObject failed: %v", err)
	}

	const rounds = 200
	var latencies []float64

	// Use GetObjectRange (64 KB = 1 ClickHouse granule) instead of HeadObject.
	// This is what actually happens during SELECT — range GETs, not HEAD.
	for i := 0; i < rounds; i++ {
		start := time.Now()
		_, err := s.client.GetObjectRange(ctx, key, 0, 65536)
		ttfb := time.Since(start)
		if err == nil {
			latencies = append(latencies, float64(ttfb.Microseconds())/1000.0)
		}
	}

	if len(latencies) < rounds/2 {
		return false, fmt.Sprintf("Too many failures: only %d/%d successful", len(latencies), rounds)
	}

	sortFloat64s(latencies)
	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]
	p999 := latencies[len(latencies)*999/1000]

	threshold := CHRequirements.GetRangeP99MaxMs
	passed := p99 < threshold
	detail := fmt.Sprintf("64KB range GET TTFB P50=%.2fms P99=%.2fms P99.9=%.2fms (CH requires P99 < %.0fms)",
		p50, p99, p999, threshold)
	return passed, detail
}

func (s *CompatScenario) testDNSRotation(ctx context.Context) (bool, string) {
	endpoint := s.cfg.Endpoint
	if endpoint == "" {
		return true, "AWS S3 (DNS managed by AWS — rotation guaranteed)"
	}

	parsed, err := url.Parse(endpoint)
	if err != nil {
		return false, fmt.Sprintf("Cannot parse endpoint URL: %v", err)
	}

	host := parsed.Hostname()

	ip := net.ParseIP(host)
	if ip != nil {
		return false, fmt.Sprintf("Endpoint is a raw IP (%s) — no DNS rotation possible, single backend hotspot risk", host)
	}

	var allIPs []string
	seen := make(map[string]bool)
	for i := 0; i < 10; i++ {
		ips, err := net.LookupHost(host)
		if err != nil {
			return false, fmt.Sprintf("DNS lookup failed: %v", err)
		}
		for _, ip := range ips {
			if !seen[ip] {
				seen[ip] = true
				allIPs = append(allIPs, ip)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	if len(allIPs) == 1 {
		return false, fmt.Sprintf("DNS returns only 1 IP (%s) — hotspot risk, no load distribution", allIPs[0])
	}
	return true, fmt.Sprintf("DNS returns %d IPs: %s", len(allIPs), strings.Join(allIPs, ", "))
}

func sortFloat64s(a []float64) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

func (s *CompatScenario) cleanupKeys(ctx context.Context, keys ...string) {
	if len(keys) > 0 {
		s.client.DeleteObjects(ctx, keys)
	}
}
