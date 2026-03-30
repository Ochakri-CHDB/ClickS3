package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type IOPSScenario struct {
	client  *S3Client
	cfg     *Config
	metrics *MetricsCollector
}

func NewIOPSScenario(client *S3Client, cfg *Config, metrics *MetricsCollector) *IOPSScenario {
	return &IOPSScenario{client: client, cfg: cfg, metrics: metrics}
}

func (s *IOPSScenario) Name() string { return "IOPS Capability Discovery" }

func (s *IOPSScenario) Description() string {
	return "Discovers peak IOPS by adaptive ramp-up, then evaluates ClickHouse suitability"
}

// StorageCapabilityReport contains measured peak capabilities of the storage backend.
type StorageCapabilityReport struct {
	PeakRawPutIOPS float64
	PeakRawGetIOPS float64
	PeakHeadIOPS   float64
	PeakCHPutIOPS  float64
	PeakCHGetIOPS  float64
	PeakGetMBps    float64
	PeakPutMBps    float64

	// Latency at operating point (60% of peak — sustainable load)
	OperatingGetP50Ms float64
	OperatingGetP99Ms float64
	OperatingPutP50Ms float64
	OperatingPutP99Ms float64

	// Saturation point
	GetSaturationThreads int
	PutSaturationThreads int

	// Error behavior
	ErrorRateAtPeak float64

	// Latency degradation curves
	GetLatencyCurve []LatencyPoint
	PutLatencyCurve []LatencyPoint
}

// LatencyPoint is one step in the concurrency ramp-up curve.
type LatencyPoint struct {
	Concurrency int     `json:"concurrency"`
	IOPS        float64 `json:"iops"`
	P50Ms       float64 `json:"p50_ms"`
	P99Ms       float64 `json:"p99_ms"`
	ErrorRate   float64 `json:"error_rate"`
}

type iopsResult struct {
	Label       string
	Concurrency int
	Duration    time.Duration
	TotalOps    int64
	SuccessOps  int64
	ErrorOps    int64
	IOPS        float64
	P50Ms       float64
	P99Ms       float64
}

type iopsSizeProfile struct {
	name    string
	putSize int64
	getSize int64
	prefix  string
}

// measureNetworkBandwidth probes raw throughput to the storage endpoint
// by uploading then downloading a 256 MB object. Returns the bottleneck
// direction (min of read, write) in MB/s. Returns 0 on failure.
func (s *IOPSScenario) measureNetworkBandwidth(ctx context.Context) float64 {
	const probeSize = 256 * 1024 * 1024 // 256 MB
	key := GenerateKey(s.cfg.Prefix + "netprobe/")
	defer s.client.DeleteObjects(ctx, []string{key})

	// Write probe
	writeStart := time.Now()
	if err := s.client.PutObject(ctx, key, probeSize); err != nil {
		fmt.Printf("  │  └─ network probe write failed: %v (skipping)\n", err)
		return 0
	}
	writeMBps := float64(probeSize) / time.Since(writeStart).Seconds() / (1024 * 1024)

	// Read probe
	readStart := time.Now()
	if _, err := s.client.GetObject(ctx, key); err != nil {
		fmt.Printf("  │  └─ network probe read failed: %v (skipping)\n", err)
		return 0
	}
	readMBps := float64(probeSize) / time.Since(readStart).Seconds() / (1024 * 1024)

	fmt.Printf("  │  └─ write=%.0f MB/s  read=%.0f MB/s  → bottleneck=%.0f MB/s\n",
		writeMBps, readMBps, minf(readMBps, writeMBps))

	return minf(readMBps, writeMBps)
}

func minf(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func (s *IOPSScenario) Run(ctx context.Context) (*ScenarioResult, error) {
	result := &ScenarioResult{
		Name: s.Name(),
	}

	// Network bandwidth probe
	fmt.Printf("\n  ┌─ Network bandwidth probe (256 MB object)...\n")
	networkBW := s.measureNetworkBandwidth(ctx)
	fmt.Printf("  └─\n")

	maxConc := s.cfg.MaxConcurrency
	stepDuration := 30 * time.Second

	profiles := []iopsSizeProfile{
		{name: "raw-4KB", putSize: 4 * 1024, getSize: 4 * 1024, prefix: "iops-raw/"},
		{name: "ch-realistic", putSize: 1024 * 1024, getSize: 64 * 1024, prefix: "iops-ch/"},
	}

	allCurves := make(map[string]map[string][]LatencyPoint)
	allPeaks := make(map[string]map[string]iopsResult)

	for _, prof := range profiles {
		fmt.Printf("\n  ╔═══════════════════════════════════════════════════════════════╗\n")
		if prof.name == "raw-4KB" {
			fmt.Printf("  ║  Discovery: Raw IOPS (4 KB) — measures API overhead         ║\n")
		} else {
			fmt.Printf("  ║  Discovery: ClickHouse Realistic (1 MB PUT, 64 KB GET)      ║\n")
		}
		fmt.Printf("  ╚═══════════════════════════════════════════════════════════════╝\n")

		profilePeaks := make(map[string]iopsResult)
		profileCurves := make(map[string][]LatencyPoint)
		fullPrefix := s.cfg.Prefix + prof.prefix

		// PUT discovery
		fmt.Printf("  ├─ PUT (%s) — adaptive ramp-up...\n", humanSize(prof.putSize))
		putCurve := s.findPeak(ctx, "put", maxConc, stepDuration, prof.putSize, fullPrefix, nil)
		putPeak := bestFromCurve(putCurve)
		profilePeaks["put"] = iopsResult{
			Label: fmt.Sprintf("PUT %s %s", prof.name, humanSize(prof.putSize)),
			IOPS: putPeak.IOPS, Concurrency: putPeak.Concurrency,
			P50Ms: putPeak.P50Ms, P99Ms: putPeak.P99Ms,
		}
		profileCurves["put"] = putCurve
		fmt.Printf("  │  └─ Peak: %.0f IOPS @ %d threads (P50=%.1fms P99=%.1fms)\n",
			putPeak.IOPS, putPeak.Concurrency, putPeak.P50Ms, putPeak.P99Ms)

		// Pre-populate for GET/HEAD
		fmt.Printf("  ├─ Pre-populating %d objects (%s)...\n", 5000, humanSize(prof.getSize))
		readPrefix := s.cfg.Prefix + prof.prefix + "read/"
		s.prepopulate(ctx, 5000, prof.getSize, readPrefix)
		readKeys := s.getReadKeys(ctx, 1000, readPrefix, fullPrefix)

		// GET discovery
		fmt.Printf("  ├─ GET (%s) — adaptive ramp-up...\n", humanSize(prof.getSize))
		getCurve := s.findPeak(ctx, "get", maxConc, stepDuration, prof.getSize, fullPrefix, readKeys)
		getPeak := bestFromCurve(getCurve)
		profilePeaks["get"] = iopsResult{
			Label: fmt.Sprintf("GET %s %s", prof.name, humanSize(prof.getSize)),
			IOPS: getPeak.IOPS, Concurrency: getPeak.Concurrency,
			P50Ms: getPeak.P50Ms, P99Ms: getPeak.P99Ms,
		}
		profileCurves["get"] = getCurve
		fmt.Printf("  │  └─ Peak: %.0f IOPS @ %d threads (P50=%.1fms P99=%.1fms)\n",
			getPeak.IOPS, getPeak.Concurrency, getPeak.P50Ms, getPeak.P99Ms)

		// Mixed discovery
		fmt.Printf("  ├─ Mixed 50/50 — adaptive ramp-up...\n")
		mixedCurve := s.findPeak(ctx, "mixed", maxConc, stepDuration, prof.putSize, fullPrefix, readKeys)
		mixedPeak := bestFromCurve(mixedCurve)
		profilePeaks["mixed"] = iopsResult{
			Label: fmt.Sprintf("Mixed %s", prof.name),
			IOPS: mixedPeak.IOPS, Concurrency: mixedPeak.Concurrency,
			P50Ms: mixedPeak.P50Ms, P99Ms: mixedPeak.P99Ms,
		}
		profileCurves["mixed"] = mixedCurve
		fmt.Printf("  │  └─ Peak: %.0f IOPS @ %d threads (P50=%.1fms P99=%.1fms)\n",
			mixedPeak.IOPS, mixedPeak.Concurrency, mixedPeak.P50Ms, mixedPeak.P99Ms)

		// HEAD discovery
		fmt.Printf("  ├─ HEAD — adaptive ramp-up...\n")
		headCurve := s.findPeak(ctx, "head", maxConc, stepDuration, 0, fullPrefix, readKeys)
		headPeak := bestFromCurve(headCurve)
		profilePeaks["head"] = iopsResult{
			Label: fmt.Sprintf("HEAD %s", prof.name),
			IOPS: headPeak.IOPS, Concurrency: headPeak.Concurrency,
			P50Ms: headPeak.P50Ms, P99Ms: headPeak.P99Ms,
		}
		profileCurves["head"] = headCurve
		fmt.Printf("  │  └─ Peak: %.0f IOPS @ %d threads (P50=%.1fms P99=%.1fms)\n",
			headPeak.IOPS, headPeak.Concurrency, headPeak.P50Ms, headPeak.P99Ms)

		// Print latency curves
		fmt.Printf("  │\n  │  PUT latency curve:\n")
		for _, p := range putCurve {
			bar := makeBar(p.IOPS, putPeak.IOPS)
			fmt.Printf("  │    %4d thr │ %8.0f IOPS │ P50=%6.1fms │ P99=%6.1fms │ err=%.1f%% │ %s\n",
				p.Concurrency, p.IOPS, p.P50Ms, p.P99Ms, p.ErrorRate*100, bar)
		}
		fmt.Printf("  │  GET latency curve:\n")
		for _, p := range getCurve {
			bar := makeBar(p.IOPS, getPeak.IOPS)
			fmt.Printf("  │    %4d thr │ %8.0f IOPS │ P50=%6.1fms │ P99=%6.1fms │ err=%.1f%% │ %s\n",
				p.Concurrency, p.IOPS, p.P50Ms, p.P99Ms, p.ErrorRate*100, bar)
		}

		allPeaks[prof.name] = profilePeaks
		allCurves[prof.name] = profileCurves
	}

	// Build the StorageCapabilityReport from CH-realistic profile
	cap := s.buildCapabilityReport(allPeaks, allCurves)

	// Print storage capability summary table
	s.printCapabilityTable(cap)

	// Build StorageCapabilities for the capacity planner
	caps := StorageCapabilities{
		PeakGetIOPS:          cap.PeakCHGetIOPS,
		PeakPutIOPS:          cap.PeakCHPutIOPS,
		PeakGetMBps:          cap.PeakGetMBps,
		PeakPutMBps:          cap.PeakPutMBps,
		GetP99AtPeakMs:       cap.OperatingGetP99Ms,
		PutP99AtPeakMs:       cap.OperatingPutP99Ms,
		GetSaturationThreads: cap.GetSaturationThreads,
		PutSaturationThreads: cap.PutSaturationThreads,
		NetworkBandwidthMBps: networkBW,
	}

	// Run capacity planner
	planner := NewCapacityPlanner(caps, s.cfg)
	configs := planner.Evaluate()
	planner.Print(configs)

	// Attach capacity data to report
	result.StorageCapabilities = &caps
	result.CapacityReport = configs

	// Build checks comparing measured capabilities against CHRequirements
	result.Checks = s.buildChecks(cap)

	// Stats for JSON report
	result.Stats = make(map[OpType]*OpStats)
	for profName, pr := range allPeaks {
		for op, r := range pr {
			key := OpType(fmt.Sprintf("%s_%s_IOPS", profName, op))
			result.Stats[key] = &OpStats{
				OpType:    OpType(r.Label),
				OpsPerSec: r.IOPS,
				P50Ms:     r.P50Ms,
				P99Ms:     r.P99Ms,
				Count:     0,
				ErrorCount: 0,
			}
		}
	}

	return result, nil
}

// findPeak uses adaptive ramp-up to discover the true peak IOPS of the storage.
// It starts at 5 concurrent goroutines and doubles until saturation is detected.
func (s *IOPSScenario) findPeak(ctx context.Context, mode string, maxConcurrency int, stepDuration time.Duration, objSize int64, prefix string, readKeys []string) []LatencyPoint {
	concurrency := 5
	var prevIOPS float64
	var baselineP99 float64
	var curve []LatencyPoint

	for concurrency <= maxConcurrency {
		if ctx.Err() != nil {
			break
		}

		r := s.runStep(ctx, mode, concurrency, stepDuration, objSize, prefix, readKeys)

		errRate := float64(r.ErrorOps) / float64(max(r.TotalOps, 1))
		point := LatencyPoint{
			Concurrency: concurrency,
			IOPS:        r.IOPS,
			P50Ms:       r.P50Ms,
			P99Ms:       r.P99Ms,
			ErrorRate:   errRate,
		}
		curve = append(curve, point)

		if baselineP99 == 0 && r.P99Ms > 0 {
			baselineP99 = r.P99Ms
		}

		// Saturation detection
		if prevIOPS > 0 {
			iopsGrowth := (r.IOPS - prevIOPS) / maxf(prevIOPS, 1)
			latencyDegradation := r.P99Ms / maxf(baselineP99, 1)

			if iopsGrowth < 0.05 || latencyDegradation > 3.0 || errRate > 0.05 {
				break
			}
		}

		prevIOPS = r.IOPS
		concurrency *= 2
	}

	return curve
}

func (s *IOPSScenario) runStep(ctx context.Context, mode string, concurrency int, duration time.Duration, objSize int64, prefix string, readKeys []string) iopsResult {
	stepCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var totalOps, successOps, errorOps atomic.Int64
	latencySlice := make([]float64, 0, 100000)
	var latMu sync.Mutex

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	start := time.Now()
	keyIdx := atomic.Int64{}

	for stepCtx.Err() == nil {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			if stepCtx.Err() != nil {
				return
			}

			opStart := time.Now()
			var err error

			switch mode {
			case "put":
				key := GenerateKey(prefix)
				err = s.client.PutObject(stepCtx, key, objSize)
			case "get":
				if len(readKeys) > 0 {
					idx := int(keyIdx.Add(1)) % len(readKeys)
					_, err = s.client.GetObject(stepCtx, readKeys[idx])
				}
			case "head":
				if len(readKeys) > 0 {
					idx := int(keyIdx.Add(1)) % len(readKeys)
					_, err = s.client.HeadObject(stepCtx, readKeys[idx])
				}
			case "mixed":
				if totalOps.Load()%2 == 0 {
					key := GenerateKey(prefix)
					err = s.client.PutObject(stepCtx, key, objSize)
				} else if len(readKeys) > 0 {
					idx := int(keyIdx.Add(1)) % len(readKeys)
					_, err = s.client.GetObject(stepCtx, readKeys[idx])
				}
			}

			latMs := float64(time.Since(opStart).Microseconds()) / 1000.0
			totalOps.Add(1)

			if err != nil {
				errorOps.Add(1)
			} else {
				successOps.Add(1)
				latMu.Lock()
				latencySlice = append(latencySlice, latMs)
				latMu.Unlock()
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	latMu.Lock()
	sort.Float64s(latencySlice)
	var p50, p99 float64
	if len(latencySlice) > 0 {
		p50 = percentile(latencySlice, 50)
		p99 = percentile(latencySlice, 99)
	}
	latMu.Unlock()

	iops := float64(successOps.Load()) / elapsed.Seconds()

	return iopsResult{
		Concurrency: concurrency,
		Duration:    elapsed,
		TotalOps:    totalOps.Load(),
		SuccessOps:  successOps.Load(),
		ErrorOps:    errorOps.Load(),
		IOPS:        iops,
		P50Ms:       p50,
		P99Ms:       p99,
	}
}

func (s *IOPSScenario) prepopulate(ctx context.Context, count int, objSize int64, prefix string) {
	sem := make(chan struct{}, 50)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			key := GenerateKey(prefix)
			s.client.PutObject(ctx, key, objSize)
		}()
	}
	wg.Wait()
}

func (s *IOPSScenario) getReadKeys(ctx context.Context, maxKeys int32, readPrefix, fallbackPrefix string) []string {
	keys, err := s.client.ListObjects(ctx, readPrefix, maxKeys)
	if err != nil || len(keys) == 0 {
		keys, _ = s.client.ListObjects(ctx, fallbackPrefix, maxKeys)
	}
	return keys
}

func (s *IOPSScenario) buildCapabilityReport(allPeaks map[string]map[string]iopsResult, allCurves map[string]map[string][]LatencyPoint) StorageCapabilityReport {
	rawPeaks := allPeaks["raw-4KB"]
	chPeaks := allPeaks["ch-realistic"]
	chCurves := allCurves["ch-realistic"]

	cap := StorageCapabilityReport{
		PeakRawPutIOPS: rawPeaks["put"].IOPS,
		PeakRawGetIOPS: rawPeaks["get"].IOPS,
		PeakHeadIOPS:   rawPeaks["head"].IOPS,
		PeakCHPutIOPS:  chPeaks["put"].IOPS,
		PeakCHGetIOPS:  chPeaks["get"].IOPS,

		PeakGetMBps:          chPeaks["get"].IOPS * 64.0 / 1024.0,
		PeakPutMBps:          chPeaks["put"].IOPS, // 1MB per op
		GetSaturationThreads: chPeaks["get"].Concurrency,
		PutSaturationThreads: chPeaks["put"].Concurrency,
	}

	if getCurve, ok := chCurves["get"]; ok {
		cap.GetLatencyCurve = getCurve
		opPoint := findOperatingPoint(getCurve, cap.PeakCHGetIOPS)
		cap.OperatingGetP50Ms = opPoint.P50Ms
		cap.OperatingGetP99Ms = opPoint.P99Ms
	}
	if putCurve, ok := chCurves["put"]; ok {
		cap.PutLatencyCurve = putCurve
		opPoint := findOperatingPoint(putCurve, cap.PeakCHPutIOPS)
		cap.OperatingPutP50Ms = opPoint.P50Ms
		cap.OperatingPutP99Ms = opPoint.P99Ms
	}

	// Error rate at peak from raw PUT (worst case)
	if rawPeaks["put"].TotalOps > 0 {
		cap.ErrorRateAtPeak = float64(rawPeaks["put"].ErrorOps) / float64(rawPeaks["put"].TotalOps)
	}

	return cap
}

// findOperatingPoint finds the latency at ~60% of peak IOPS (sustainable operating point).
func findOperatingPoint(curve []LatencyPoint, peakIOPS float64) LatencyPoint {
	target := peakIOPS * 0.6
	var best LatencyPoint
	bestDist := peakIOPS
	for _, p := range curve {
		dist := p.IOPS - target
		if dist < 0 {
			dist = -dist
		}
		if dist < bestDist {
			bestDist = dist
			best = p
		}
	}
	return best
}

func (s *IOPSScenario) printCapabilityTable(cap StorageCapabilityReport) {
	req := CHRequirements

	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────────────────────────────────┐")
	fmt.Println("  │  Storage Capability Profile                                         │")
	fmt.Println("  ├──────────────────────────────┬───────────────┬─────────────────────┤")
	fmt.Println("  │  Metric                      │  Measured     │  CH Requires        │")
	fmt.Println("  ├──────────────────────────────┼───────────────┼─────────────────────┤")

	s.printCapRow("Peak GET IOPS (64KB)", cap.PeakCHGetIOPS, fmt.Sprintf(">= %.0f", req.MinGetIOPS), cap.PeakCHGetIOPS >= req.MinGetIOPS)
	s.printCapRow("Peak PUT IOPS (1MB)", cap.PeakCHPutIOPS, fmt.Sprintf(">= %.0f", req.MinPutIOPS), cap.PeakCHPutIOPS >= req.MinPutIOPS)
	s.printCapRow("Peak HEAD IOPS", cap.PeakHeadIOPS, fmt.Sprintf(">= %.0f", req.MinHeadIOPS), cap.PeakHeadIOPS >= req.MinHeadIOPS)
	s.printCapRow("Peak GET throughput", cap.PeakGetMBps, fmt.Sprintf(">= %.0f MB/s", req.MinGetMBps), cap.PeakGetMBps >= req.MinGetMBps)
	s.printCapRow("Peak PUT throughput", cap.PeakPutMBps, fmt.Sprintf(">= %.0f MB/s", req.MinPutMBps), cap.PeakPutMBps >= req.MinPutMBps)
	s.printCapRowMs("GET P99 @ operating load", cap.OperatingGetP99Ms, fmt.Sprintf("< %.0f ms", req.GetRangeP99MaxMs), cap.OperatingGetP99Ms < req.GetRangeP99MaxMs)
	s.printCapRowMs("PUT P99 @ operating load", cap.OperatingPutP99Ms, fmt.Sprintf("< %.0f ms", req.PutSmallP99MaxMs), cap.OperatingPutP99Ms < req.PutSmallP99MaxMs)

	fmt.Printf("  │ %-28s │ %5d threads │ %-19s │\n", "GET saturation point", cap.GetSaturationThreads, "-")
	fmt.Printf("  │ %-28s │ %5d threads │ %-19s │\n", "PUT saturation point", cap.PutSaturationThreads, "-")

	fmt.Println("  ├──────────────────────────────┴───────────────┴─────────────────────┤")

	allPass := cap.PeakCHGetIOPS >= req.MinGetIOPS &&
		cap.PeakCHPutIOPS >= req.MinPutIOPS &&
		cap.PeakHeadIOPS >= req.MinHeadIOPS &&
		cap.PeakGetMBps >= req.MinGetMBps &&
		cap.PeakPutMBps >= req.MinPutMBps &&
		cap.OperatingGetP99Ms < req.GetRangeP99MaxMs &&
		cap.OperatingPutP99Ms < req.PutSmallP99MaxMs

	if allPass {
		fmt.Println("  │  VERDICT: SUITABLE for ClickHouse at 15M rows/min ingestion       │")
	} else {
		fmt.Println("  │  VERDICT: INSUFFICIENT — some ClickHouse requirements not met      │")
	}
	fmt.Println("  └─────────────────────────────────────────────────────────────────────┘")
	fmt.Println()
}

func (s *IOPSScenario) printCapRow(metric string, measured float64, requirement string, pass bool) {
	icon := "✓"
	if !pass {
		icon = "✗"
	}
	fmt.Printf("  │ %-28s │ %9.0f %s   │ %-19s │\n", metric, measured, icon, requirement)
}

func (s *IOPSScenario) printCapRowMs(metric string, measured float64, requirement string, pass bool) {
	icon := "✓"
	if !pass {
		icon = "✗"
	}
	fmt.Printf("  │ %-28s │ %7.1f ms %s │ %-19s │\n", metric, measured, icon, requirement)
}

func (s *IOPSScenario) buildChecks(cap StorageCapabilityReport) []Check {
	req := CHRequirements
	return []Check{
		{
			Name:   fmt.Sprintf("GET IOPS sufficient for ClickHouse (need %.0f, got %.0f)", req.MinGetIOPS, cap.PeakCHGetIOPS),
			Passed: cap.PeakCHGetIOPS >= req.MinGetIOPS,
			Detail: fmt.Sprintf("peak %.0f IOPS @ %d threads", cap.PeakCHGetIOPS, cap.GetSaturationThreads),
		},
		{
			Name:   fmt.Sprintf("PUT IOPS sufficient for ClickHouse (need %.0f, got %.0f)", req.MinPutIOPS, cap.PeakCHPutIOPS),
			Passed: cap.PeakCHPutIOPS >= req.MinPutIOPS,
			Detail: fmt.Sprintf("peak %.0f IOPS @ %d threads", cap.PeakCHPutIOPS, cap.PutSaturationThreads),
		},
		{
			Name:   fmt.Sprintf("HEAD IOPS sufficient for ClickHouse (need %.0f, got %.0f)", req.MinHeadIOPS, cap.PeakHeadIOPS),
			Passed: cap.PeakHeadIOPS >= req.MinHeadIOPS,
			Detail: fmt.Sprintf("peak %.0f IOPS", cap.PeakHeadIOPS),
		},
		{
			Name:   fmt.Sprintf("GET throughput sufficient (need %.0f MB/s, got %.0f MB/s)", req.MinGetMBps, cap.PeakGetMBps),
			Passed: cap.PeakGetMBps >= req.MinGetMBps,
			Detail: fmt.Sprintf("%.0f MB/s at peak GET IOPS", cap.PeakGetMBps),
		},
		{
			Name:   fmt.Sprintf("PUT throughput sufficient (need %.0f MB/s, got %.0f MB/s)", req.MinPutMBps, cap.PeakPutMBps),
			Passed: cap.PeakPutMBps >= req.MinPutMBps,
			Detail: fmt.Sprintf("%.0f MB/s at peak PUT IOPS", cap.PeakPutMBps),
		},
		{
			Name:   fmt.Sprintf("GET latency at operating load (need P99 < %.0fms, got %.1fms)", req.GetRangeP99MaxMs, cap.OperatingGetP99Ms),
			Passed: cap.OperatingGetP99Ms < req.GetRangeP99MaxMs,
			Detail: "measured at 60% of peak IOPS",
		},
		{
			Name:   fmt.Sprintf("PUT latency at operating load (need P99 < %.0fms, got %.1fms)", req.PutSmallP99MaxMs, cap.OperatingPutP99Ms),
			Passed: cap.OperatingPutP99Ms < req.PutSmallP99MaxMs,
			Detail: "measured at 60% of peak IOPS",
		},
		{
			Name:   "PUT error rate < 5% at peak",
			Passed: cap.ErrorRateAtPeak < 0.05,
			Detail: fmt.Sprintf("%.1f%%", cap.ErrorRateAtPeak*100),
		},
	}
}

func bestFromCurve(curve []LatencyPoint) iopsResult {
	var best iopsResult
	for _, p := range curve {
		if p.IOPS > best.IOPS {
			best = iopsResult{
				Concurrency: p.Concurrency,
				IOPS:        p.IOPS,
				P50Ms:       p.P50Ms,
				P99Ms:       p.P99Ms,
			}
		}
	}
	return best
}

func findPeak(results []iopsResult) iopsResult {
	var best iopsResult
	for _, r := range results {
		if r.IOPS > best.IOPS {
			best = r
		}
	}
	return best
}

func makeBar(value, maxValue float64) string {
	if maxValue == 0 {
		return ""
	}
	width := 30
	filled := int(value / maxValue * float64(width))
	if filled > width {
		filled = width
	}
	bar := ""
	for i := 0; i < filled; i++ {
		bar += "█"
	}
	for i := filled; i < width; i++ {
		bar += "░"
	}
	return bar
}

func humanSize(bytes int64) string {
	switch {
	case bytes >= 1024*1024:
		return fmt.Sprintf("%d MB", bytes/(1024*1024))
	case bytes >= 1024:
		return fmt.Sprintf("%d KB", bytes/1024)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
