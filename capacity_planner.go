package main

import (
	"fmt"
	"math"
)

// StorageCapabilities holds measured peak capabilities from the IOPS scenario.
type StorageCapabilities struct {
	PeakGetIOPS          float64 `json:"peak_get_iops"`
	PeakPutIOPS          float64 `json:"peak_put_iops"`
	PeakGetMBps          float64 `json:"peak_get_mbps"`
	PeakPutMBps          float64 `json:"peak_put_mbps"`
	GetP99AtPeakMs       float64 `json:"get_p99_at_peak_ms"`
	PutP99AtPeakMs       float64 `json:"put_p99_at_peak_ms"`
	GetSaturationThreads int     `json:"get_saturation_threads"`
	PutSaturationThreads int     `json:"put_saturation_threads"`
	NetworkBandwidthMBps float64 `json:"network_bandwidth_mbps"`
}

// iopsLookup: measured P90 IOPS per replica from representative workloads.
// Source: multi-region, 3-replica services, >=60% memory utilization.
// Planning value: P90 * iopsP90SafetyMultiplier = conservative peak estimate.
// Do NOT use P99 directly — it is dominated by burst outliers.
var iopsLookup = []struct {
	RAMGiB           int
	P90GetPerReplica float64
	P90PutPerReplica float64
	MeasuredHours    int
}{
	{8, 49, 14, 95000},
	{16, 116, 27, 72000},
	{32, 106, 26, 64000},
	{64, 200, 61, 50000},
	{120, 148, 46, 46000},
	{160, 298, 143, 6000},
	{236, 121, 30, 13000},
	{340, 83, 19, 624},
	{360, 115, 45, 1347},
}

const iopsP90SafetyMultiplier = 2.5

// interpolateIOPS returns the P90 GET and PUT IOPS per replica for a given
// RAM size, using linear interpolation between calibration points. For RAM
// below the minimum entry, returns the minimum entry values. For RAM above
// the maximum entry (360 GiB), returns the maximum entry values (no
// extrapolation).
func interpolateIOPS(ramGiB int) (getIOPS, putIOPS float64) {
	if ramGiB <= iopsLookup[0].RAMGiB {
		return iopsLookup[0].P90GetPerReplica, iopsLookup[0].P90PutPerReplica
	}
	for i := 1; i < len(iopsLookup); i++ {
		lo, hi := iopsLookup[i-1], iopsLookup[i]
		if ramGiB <= hi.RAMGiB {
			frac := float64(ramGiB-lo.RAMGiB) / float64(hi.RAMGiB-lo.RAMGiB)
			return lo.P90GetPerReplica + frac*(hi.P90GetPerReplica-lo.P90GetPerReplica),
				lo.P90PutPerReplica + frac*(hi.P90PutPerReplica-lo.P90PutPerReplica)
		}
	}
	last := iopsLookup[len(iopsLookup)-1]
	return last.P90GetPerReplica, last.P90PutPerReplica
}

// interpolatedConfidence returns the minimum MeasuredHours for the two
// bracketing lookup entries, or the exact entry's hours if ramGiB matches.
func interpolatedConfidence(ramGiB int) int {
	if ramGiB <= iopsLookup[0].RAMGiB {
		return iopsLookup[0].MeasuredHours
	}
	for i := 1; i < len(iopsLookup); i++ {
		lo, hi := iopsLookup[i-1], iopsLookup[i]
		if ramGiB <= hi.RAMGiB {
			if lo.MeasuredHours < hi.MeasuredHours {
				return lo.MeasuredHours
			}
			return hi.MeasuredHours
		}
	}
	return iopsLookup[len(iopsLookup)-1].MeasuredHours
}

const (
	getKBPerOp = 64.0 // average GET object size (KB) — 64KB granule
	putMBPerOp = 1.0  // average PUT object size (MB) — async flush
)

// StandardRAMSizes: complete 60-GiB step matrix for capacity planning (GiB).
var StandardRAMSizes = []int{8, 16, 32, 48, 60, 64, 96, 120, 180, 240, 360, 480, 720}

// StandardReplicaCounts are the replica counts to evaluate.
var StandardReplicaCounts = []int{1, 2, 3, 4, 6, 8, 10, 12, 15, 20}

// CHConfig represents the evaluation of one ClickHouse configuration
// against the measured storage capabilities.
type CHConfig struct {
	RAMPerReplicaGiB int     `json:"ram_per_replica_gib"`
	Replicas         int     `json:"replicas"`
	RequiredGetIOPS  float64 `json:"required_get_iops"`
	RequiredPutIOPS  float64 `json:"required_put_iops"`
	RequiredGetMBps  float64 `json:"required_get_mbps"`
	RequiredPutMBps  float64 `json:"required_put_mbps"`

	IOPSConstrained    bool    `json:"iops_constrained"`
	NetworkConstrained bool    `json:"network_constrained"`
	LatencyConstrained bool    `json:"latency_constrained"`
	Supported          bool    `json:"supported"`
	LimitingFactor     string  `json:"limiting_factor"`
	HeadroomPct        float64 `json:"headroom_pct"`
	LowConfidence      bool    `json:"low_confidence"`
}

// CapacityPlanner evaluates which ClickHouse configurations a storage
// backend can support, based on measured capabilities.
type CapacityPlanner struct {
	caps StorageCapabilities
	cfg  *Config
}

func NewCapacityPlanner(caps StorageCapabilities, cfg *Config) *CapacityPlanner {
	return &CapacityPlanner{caps: caps, cfg: cfg}
}

func (p *CapacityPlanner) Evaluate() []CHConfig {
	var results []CHConfig
	for _, ram := range StandardRAMSizes {
		for _, replicas := range StandardReplicaCounts {
			cfg := p.evaluate(ram, replicas)
			results = append(results, cfg)
		}
	}
	return results
}

func (p *CapacityPlanner) evaluate(ramGiB, replicas int) CHConfig {
	cfg := CHConfig{
		RAMPerReplicaGiB: ramGiB,
		Replicas:         replicas,
	}

	replicasF := float64(replicas)

	getPerReplica, putPerReplica := interpolateIOPS(ramGiB)

	cfg.RequiredGetIOPS = replicasF * getPerReplica * iopsP90SafetyMultiplier
	cfg.RequiredPutIOPS = replicasF * putPerReplica * iopsP90SafetyMultiplier
	cfg.RequiredGetMBps = cfg.RequiredGetIOPS * getKBPerOp / 1024.0
	cfg.RequiredPutMBps = cfg.RequiredPutIOPS * putMBPerOp

	cfg.LowConfidence = interpolatedConfidence(ramGiB) < 1000

	getIOPSHeadroom := (p.caps.PeakGetIOPS - cfg.RequiredGetIOPS) / math.Max(p.caps.PeakGetIOPS, 1) * 100
	putIOPSHeadroom := (p.caps.PeakPutIOPS - cfg.RequiredPutIOPS) / math.Max(p.caps.PeakPutIOPS, 1) * 100
	getBWHeadroom := (p.caps.PeakGetMBps - cfg.RequiredGetMBps) / math.Max(p.caps.PeakGetMBps, 1) * 100
	putBWHeadroom := (p.caps.PeakPutMBps - cfg.RequiredPutMBps) / math.Max(p.caps.PeakPutMBps, 1) * 100

	netHeadroom := 100.0
	if p.caps.NetworkBandwidthMBps > 0 {
		totalBW := cfg.RequiredGetMBps + cfg.RequiredPutMBps
		netHeadroom = (p.caps.NetworkBandwidthMBps - totalBW) / math.Max(p.caps.NetworkBandwidthMBps, 1) * 100
	}

	getLoadFactor := cfg.RequiredGetIOPS / math.Max(p.caps.PeakGetIOPS, 1)
	expectedGetP99 := p.caps.GetP99AtPeakMs * getLoadFactor * 1.5
	latencyOK := expectedGetP99 < float64(CHRequirements.GetRangeP99MaxMs)

	minHeadroom := math.Min(
		math.Min(getIOPSHeadroom, putIOPSHeadroom),
		math.Min(getBWHeadroom, math.Min(putBWHeadroom, netHeadroom)),
	)

	cfg.Supported = getIOPSHeadroom >= 0 &&
		putIOPSHeadroom >= 0 &&
		getBWHeadroom >= 0 &&
		putBWHeadroom >= 0 &&
		netHeadroom >= 0 &&
		latencyOK

	cfg.HeadroomPct = minHeadroom
	cfg.LatencyConstrained = !latencyOK

	switch {
	case getIOPSHeadroom <= minHeadroom+0.01 && getIOPSHeadroom < 0:
		cfg.LimitingFactor = "GET_IOPS"
		cfg.IOPSConstrained = true
	case putIOPSHeadroom <= minHeadroom+0.01 && putIOPSHeadroom < 0:
		cfg.LimitingFactor = "PUT_IOPS"
		cfg.IOPSConstrained = true
	case getBWHeadroom <= minHeadroom+0.01 && getBWHeadroom < 0:
		cfg.LimitingFactor = "GET_BW"
		cfg.NetworkConstrained = true
	case putBWHeadroom <= minHeadroom+0.01 && putBWHeadroom < 0:
		cfg.LimitingFactor = "PUT_BW"
		cfg.NetworkConstrained = true
	case netHeadroom <= minHeadroom+0.01 && netHeadroom < 0:
		cfg.LimitingFactor = "network"
		cfg.NetworkConstrained = true
	case !latencyOK:
		cfg.LimitingFactor = "latency"
		cfg.LatencyConstrained = true
	default:
		cfg.LimitingFactor = "none"
	}

	return cfg
}

// Print outputs the capacity planning results. The full matrix table is
// printed only in verbose mode or when a scenario preset is active. The
// max-replicas summary table is always printed.
func (p *CapacityPlanner) Print(results []CHConfig) {
	fmt.Println()
	fmt.Println("  ┌────────────────────────────────────────────────────────────────────────────┐")
	fmt.Println("  │  ClickHouse Capacity Planning — What this storage can support              │")
	fmt.Println("  ├────────────────────────────────────────────────────────────────────────────┤")
	fmt.Printf("  │  Storage peaks: GET %6.0f IOPS / %5.0f MB/s  │  PUT %6.0f IOPS / %5.0f MB/s │\n",
		p.caps.PeakGetIOPS, p.caps.PeakGetMBps, p.caps.PeakPutIOPS, p.caps.PeakPutMBps)
	if p.caps.NetworkBandwidthMBps > 0 {
		fmt.Printf("  │  Network bandwidth: %.0f MB/s (bottleneck direction)                       │\n",
			p.caps.NetworkBandwidthMBps)
	}
	fmt.Printf("  │  Planning model: P90 x %.1f safety multiplier                               │\n",
		iopsP90SafetyMultiplier)
	fmt.Println("  └────────────────────────────────────────────────────────────────────────────┘")

	showMatrix := p.cfg.Verbose || p.cfg.ScenarioPreset != ""

	if showMatrix {
		p.printMatrixTable(results)
	}

	p.printMaxReplicasTable(results)
}

// printMatrixTable prints the full RAM x replicas matrix grouped by RAM size.
func (p *CapacityPlanner) printMatrixTable(results []CHConfig) {
	byRAM := make(map[int]map[int]*CHConfig)
	for i := range results {
		c := &results[i]
		if byRAM[c.RAMPerReplicaGiB] == nil {
			byRAM[c.RAMPerReplicaGiB] = make(map[int]*CHConfig)
		}
		byRAM[c.RAMPerReplicaGiB][c.Replicas] = c
	}

	const colW = 6
	nCols := len(StandardReplicaCounts)
	bodyW := 10 + nCols*colW // "  GET dem " + data

	fmt.Println()
	// Header
	fmt.Printf("  ┌──────────┬")
	for i := 0; i < bodyW; i++ {
		fmt.Printf("─")
	}
	fmt.Printf("┐\n")

	fmt.Printf("  │ RAM/rep  │  Replicas:")
	for _, r := range StandardReplicaCounts {
		fmt.Printf("%*d", colW, r)
	}
	fmt.Printf(" │\n")

	fmt.Printf("  ├──────────┼")
	for i := 0; i < bodyW; i++ {
		fmt.Printf("─")
	}
	fmt.Printf("┤\n")

	sep := func() {
		fmt.Printf("  ├──────────┼")
		for i := 0; i < bodyW; i++ {
			fmt.Printf("─")
		}
		fmt.Printf("┤\n")
	}

	for i, ram := range StandardRAMSizes {
		if i > 0 {
			sep()
		}
		repMap := byRAM[ram]

		fmt.Printf("  │ %4d GiB │  GET dem ", ram)
		for _, r := range StandardReplicaCounts {
			if c, ok := repMap[r]; ok {
				fmt.Printf("%*.*f", colW, 0, c.RequiredGetIOPS)
			} else {
				fmt.Printf("%*s", colW, "-")
			}
		}
		fmt.Printf(" │\n")

		fmt.Printf("  │          │  PUT dem ")
		for _, r := range StandardReplicaCounts {
			if c, ok := repMap[r]; ok {
				fmt.Printf("%*.*f", colW, 0, c.RequiredPutIOPS)
			} else {
				fmt.Printf("%*s", colW, "-")
			}
		}
		fmt.Printf(" │\n")

		fmt.Printf("  │          │  Verdict ")
		for _, r := range StandardReplicaCounts {
			c, ok := repMap[r]
			if !ok {
				fmt.Printf("%*s", colW, "-")
				continue
			}
			cell := verdictCell(c)
			fmt.Printf("%*s", colW, cell)
		}
		fmt.Printf(" │\n")
	}

	fmt.Printf("  └──────────┴")
	for i := 0; i < bodyW; i++ {
		fmt.Printf("─")
	}
	fmt.Printf("┘\n")
	fmt.Println("    OK = supported (>20% headroom)  ok = supported (<20% headroom)  NO:xx = bottleneck")
	fmt.Println("    \u2020 = low confidence (<1000 measured hours)")
}

func verdictCell(c *CHConfig) string {
	suffix := ""
	if c.LowConfidence {
		suffix = "\u2020"
	}
	if c.Supported {
		if c.HeadroomPct > 20 {
			return "OK" + suffix
		}
		return "ok" + suffix
	}
	short := ""
	switch c.LimitingFactor {
	case "GET_IOPS":
		short = "GT"
	case "PUT_IOPS":
		short = "PT"
	case "GET_BW":
		short = "GB"
	case "PUT_BW":
		short = "PB"
	case "network":
		short = "NW"
	case "latency":
		short = "LT"
	default:
		short = "??"
	}
	return "NO:" + short + suffix
}

// printMaxReplicasTable prints a compact summary: for each RAM size, the
// maximum number of replicas this storage can support.
func (p *CapacityPlanner) printMaxReplicasTable(results []CHConfig) {
	// Build map: ramGiB -> ordered slice of configs
	byRAM := make(map[int][]CHConfig)
	for _, c := range results {
		byRAM[c.RAMPerReplicaGiB] = append(byRAM[c.RAMPerReplicaGiB], c)
	}

	fmt.Println()
	fmt.Println("  Maximum supported replicas by RAM size:")
	fmt.Println("  ┌──────────────┬───────────────┬────────────────────┐")
	fmt.Println("  │ RAM/replica  │ Max replicas  │ Bottleneck         │")
	fmt.Println("  ├──────────────┼───────────────┼────────────────────┤")

	for _, ram := range StandardRAMSizes {
		configs := byRAM[ram]
		maxRep := 0
		var firstFailFactor string
		for _, c := range configs {
			if c.Supported {
				if c.Replicas > maxRep {
					maxRep = c.Replicas
				}
			}
		}
		// Find the limiting factor at maxRep+1
		for _, c := range configs {
			if !c.Supported && c.Replicas == maxRep+1 {
				firstFailFactor = c.LimitingFactor
				break
			}
		}
		if firstFailFactor == "" && maxRep > 0 {
			firstFailFactor = "-"
		}

		conf := ""
		if maxRep > 0 && interpolatedConfidence(ram) < 1000 {
			conf = " \u2020"
		}

		bottleneck := "-"
		if maxRep == 0 {
			bottleneck = "none supported"
			// find the factor from replica=1
			for _, c := range configs {
				if c.Replicas == 1 && !c.Supported {
					bottleneck = fmt.Sprintf("%s at 1+", c.LimitingFactor)
					break
				}
			}
		} else if firstFailFactor != "" && firstFailFactor != "-" {
			bottleneck = fmt.Sprintf("%s at %d+", firstFailFactor, maxRep+1)
		} else if maxRep == StandardReplicaCounts[len(StandardReplicaCounts)-1] {
			bottleneck = "all tested OK"
		}

		fmt.Printf("  │  %4d GiB    │      %2d%s      │  %-18s│\n",
			ram, maxRep, conf, bottleneck)
	}

	fmt.Println("  └──────────────┴───────────────┴────────────────────┘")
	fmt.Println("    † = low confidence (<1000 measured hours) — extrapolate with caution")
	fmt.Println()
}

func pluralS(n int) string {
	if n == 1 {
		return " "
	}
	return "s"
}
