package main

import (
	"encoding/json"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type OpType string

const (
	OpPutSmall         OpType = "PUT_small"
	OpPutLarge         OpType = "PUT_large"
	OpCreateMultipart  OpType = "CreateMultipart"
	OpUploadPart       OpType = "UploadPart"
	OpCompleteMultipart OpType = "CompleteMultipart"
	OpAbortMultipart   OpType = "AbortMultipart"
	OpGetFull          OpType = "GET_full_small"
	OpGetRange         OpType = "GET_range"
	OpHeadObject       OpType = "HEAD"
	OpDeleteObjects    OpType = "DeleteObjects"
	OpDeleteObject     OpType = "DELETE_single"
	OpListObjects      OpType = "ListObjectsV2"
	OpListMultipart    OpType = "ListMultipartUploads"
)

type Sample struct {
	LatencyMs float64
	Bytes     int64
	Error     string
	Timestamp time.Time
}

type OpStats struct {
	OpType           OpType  `json:"op_type"`
	Count            int64   `json:"count"`
	SuccessCount     int64   `json:"success_count"`
	ErrorCount       int64   `json:"error_count"`
	BytesTransferred int64   `json:"bytes_transferred"`
	P50Ms            float64 `json:"p50_ms"`
	P90Ms            float64 `json:"p90_ms"`
	P95Ms            float64 `json:"p95_ms"`
	P99Ms            float64 `json:"p99_ms"`
	MinMs            float64 `json:"min_ms"`
	MaxMs            float64 `json:"max_ms"`
	AvgMs            float64 `json:"avg_ms"`
	ThroughputMBps   float64 `json:"throughput_mbps"`
	OpsPerSec        float64 `json:"ops_per_sec"`
}

type MetricsCollector struct {
	mu       sync.Mutex
	samples  map[OpType][]Sample
	startTime time.Time

	PeakConcurrent          atomic.Int64
	currentConcurrent       atomic.Int64
	ConsistencyViolations   atomic.Int64
	TimeoutViolations       atomic.Int64
	ErrorsByType            sync.Map

	MultipartInitiated  atomic.Int64
	MultipartCompleted  atomic.Int64
	MultipartAborted    atomic.Int64
	MultipartFailed     atomic.Int64
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		samples:   make(map[OpType][]Sample),
		startTime: time.Now(),
	}
}

func (m *MetricsCollector) RecordOp(op OpType, latency time.Duration, bytes int64, err error) {
	s := Sample{
		LatencyMs: float64(latency.Microseconds()) / 1000.0,
		Bytes:     bytes,
		Timestamp: time.Now(),
	}
	if err != nil {
		s.Error = err.Error()
		m.trackError(err.Error())
	}

	m.mu.Lock()
	m.samples[op] = append(m.samples[op], s)
	m.mu.Unlock()
}

func (m *MetricsCollector) TrackConcurrency(delta int64) {
	current := m.currentConcurrent.Add(delta)
	for {
		peak := m.PeakConcurrent.Load()
		if current <= peak || m.PeakConcurrent.CompareAndSwap(peak, current) {
			break
		}
	}
}

func (m *MetricsCollector) trackError(errStr string) {
	val, _ := m.ErrorsByType.LoadOrStore(errStr, new(atomic.Int64))
	val.(*atomic.Int64).Add(1)
}

func (m *MetricsCollector) GetStats(op OpType) *OpStats {
	m.mu.Lock()
	samples := make([]Sample, len(m.samples[op]))
	copy(samples, m.samples[op])
	m.mu.Unlock()

	if len(samples) == 0 {
		return &OpStats{OpType: op}
	}

	elapsed := time.Since(m.startTime).Seconds()
	if elapsed == 0 {
		elapsed = 1
	}

	var successLatencies []float64
	var totalBytes int64
	var errorCount int64

	for _, s := range samples {
		if s.Error == "" {
			successLatencies = append(successLatencies, s.LatencyMs)
		} else {
			errorCount++
		}
		totalBytes += s.Bytes
	}

	sort.Float64s(successLatencies)

	stats := &OpStats{
		OpType:           op,
		Count:            int64(len(samples)),
		SuccessCount:     int64(len(successLatencies)),
		ErrorCount:       errorCount,
		BytesTransferred: totalBytes,
		OpsPerSec:        float64(len(samples)) / elapsed,
		ThroughputMBps:   float64(totalBytes) / (1024 * 1024) / elapsed,
	}

	if len(successLatencies) > 0 {
		stats.MinMs = successLatencies[0]
		stats.MaxMs = successLatencies[len(successLatencies)-1]
		stats.AvgMs = avg(successLatencies)
		stats.P50Ms = percentile(successLatencies, 50)
		stats.P90Ms = percentile(successLatencies, 90)
		stats.P95Ms = percentile(successLatencies, 95)
		stats.P99Ms = percentile(successLatencies, 99)
	}

	return stats
}

func (m *MetricsCollector) GetAllStats() map[OpType]*OpStats {
	m.mu.Lock()
	ops := make([]OpType, 0, len(m.samples))
	for op := range m.samples {
		ops = append(ops, op)
	}
	m.mu.Unlock()

	result := make(map[OpType]*OpStats)
	for _, op := range ops {
		result[op] = m.GetStats(op)
	}
	return result
}

func (m *MetricsCollector) GetErrorSummary() map[string]int64 {
	errors := make(map[string]int64)
	m.ErrorsByType.Range(func(key, value any) bool {
		errors[key.(string)] = value.(*atomic.Int64).Load()
		return true
	})
	return errors
}

func (m *MetricsCollector) Reset() {
	m.mu.Lock()
	m.samples = make(map[OpType][]Sample)
	m.mu.Unlock()
	m.startTime = time.Now()
	m.PeakConcurrent.Store(0)
	m.currentConcurrent.Store(0)
	m.ConsistencyViolations.Store(0)
	m.TimeoutViolations.Store(0)
	m.MultipartInitiated.Store(0)
	m.MultipartCompleted.Store(0)
	m.MultipartAborted.Store(0)
	m.MultipartFailed.Store(0)
}

func (m *MetricsCollector) ToJSON() ([]byte, error) {
	report := struct {
		Stats                map[OpType]*OpStats `json:"operation_stats"`
		PeakConcurrent       int64              `json:"peak_concurrent_connections"`
		ConsistencyViolations int64             `json:"consistency_violations"`
		TimeoutViolations    int64              `json:"timeout_violations_gt_30s"`
		MultipartInitiated   int64              `json:"multipart_uploads_initiated"`
		MultipartCompleted   int64              `json:"multipart_uploads_completed"`
		MultipartAborted     int64              `json:"multipart_uploads_aborted"`
		MultipartFailed      int64              `json:"multipart_uploads_failed"`
		Errors               map[string]int64   `json:"errors_by_type"`
	}{
		Stats:                 m.GetAllStats(),
		PeakConcurrent:        m.PeakConcurrent.Load(),
		ConsistencyViolations: m.ConsistencyViolations.Load(),
		TimeoutViolations:     m.TimeoutViolations.Load(),
		MultipartInitiated:    m.MultipartInitiated.Load(),
		MultipartCompleted:    m.MultipartCompleted.Load(),
		MultipartAborted:      m.MultipartAborted.Load(),
		MultipartFailed:       m.MultipartFailed.Load(),
		Errors:                m.GetErrorSummary(),
	}
	return json.MarshalIndent(report, "", "  ")
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p / 100.0) * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

func avg(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}
