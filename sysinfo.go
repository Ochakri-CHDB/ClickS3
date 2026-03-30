package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// ServerResources describes the machine running the benchmark
type ServerResources struct {
	Hostname   string   `json:"hostname"`
	OS         string   `json:"os"`
	Arch       string   `json:"arch"`
	CPUCores   int      `json:"cpu_cores"`
	TotalRAMGB float64  `json:"total_ram_gb"`
	GoMaxProcs int      `json:"gomaxprocs"`
	NetworkIPs []string `json:"network_ips"`
	KernelInfo string   `json:"kernel_info"`

	// ScaleFactor: min(cpuFactor, ramFactor), each capped at 4.0.
	// Used ONLY for concurrency/thread scaling — not for IOPS or throughput targets.
	ScaleFactor float64 `json:"scale_factor"`
}

const (
	referenceCPU   = 8
	referenceRAMGB = 16.0
)

// DetectResources auto-detects the server's hardware capabilities
func DetectResources() *ServerResources {
	res := &ServerResources{
		OS:         runtime.GOOS,
		Arch:       runtime.GOARCH,
		CPUCores:   runtime.NumCPU(),
		GoMaxProcs: runtime.GOMAXPROCS(0),
	}

	res.Hostname, _ = os.Hostname()
	res.TotalRAMGB = detectRAM()
	res.NetworkIPs = detectNetworkIPs()
	res.KernelInfo = detectKernel()

	cpuFactor := float64(res.CPUCores) / float64(referenceCPU)
	if cpuFactor > 4.0 {
		cpuFactor = 4.0
	}
	ramFactor := res.TotalRAMGB / referenceRAMGB
	if ramFactor > 4.0 {
		ramFactor = 4.0
	}

	res.ScaleFactor = min64(cpuFactor, ramFactor)
	if res.ScaleFactor < 0.1 {
		res.ScaleFactor = 0.1
	}

	return res
}

// ScaleThreads adjusts thread count based on available resources
func (r *ServerResources) ScaleThreads(requested int) int {
	scaled := int(float64(requested) * r.ScaleFactor)
	if scaled < 1 {
		return 1
	}
	maxThreads := r.CPUCores * 4
	if scaled > maxThreads {
		return maxThreads
	}
	return scaled
}

func (r *ServerResources) PrintBanner() {
	fmt.Printf("  ┌─────────────────────────────────────────────────────────┐\n")
	fmt.Printf("  │ Server Resources                                       │\n")
	fmt.Printf("  ├─────────────────────────────────────────────────────────┤\n")
	fmt.Printf("  │ Hostname:   %-45s│\n", truncate(r.Hostname, 45))
	fmt.Printf("  │ OS/Arch:    %-45s│\n", fmt.Sprintf("%s/%s", r.OS, r.Arch))
	fmt.Printf("  │ CPU Cores:  %-45s│\n", fmt.Sprintf("%d (GOMAXPROCS=%d)", r.CPUCores, r.GoMaxProcs))
	fmt.Printf("  │ RAM:        %-45s│\n", fmt.Sprintf("%.1f GB", r.TotalRAMGB))
	if r.KernelInfo != "" {
		fmt.Printf("  │ Kernel:     %-45s│\n", truncate(r.KernelInfo, 45))
	}
	for i, ip := range r.NetworkIPs {
		if i == 0 {
			fmt.Printf("  │ Network:    %-45s│\n", ip)
		} else if i < 3 {
			fmt.Printf("  │             %-45s│\n", ip)
		}
	}
	fmt.Printf("  ├─────────────────────────────────────────────────────────┤\n")

	label := "NOMINAL"
	if r.ScaleFactor < 0.5 {
		label = "LIMITED — thread counts reduced"
	} else if r.ScaleFactor > 1.5 {
		label = "HIGH — above reference"
	}
	fmt.Printf("  │ Scale:      %-45s│\n",
		fmt.Sprintf("%.2fx threads (ref: %d vCPU / %.0f GB) — %s",
			r.ScaleFactor, referenceCPU, referenceRAMGB, label))
	fmt.Printf("  │ Mode:       %-45s│\n", "discovery (measure → evaluate)")
	fmt.Printf("  └─────────────────────────────────────────────────────────┘\n")
}

func detectRAM() float64 {
	switch runtime.GOOS {
	case "linux":
		return detectRAMLinux()
	case "darwin":
		return detectRAMDarwin()
	default:
		return 0
	}
}

func detectRAMLinux() float64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseFloat(fields[1], 64)
				if err == nil {
					return kb / (1024 * 1024)
				}
			}
		}
	}
	return 0
}

func detectRAMDarwin() float64 {
	out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
	if err != nil {
		return 0
	}
	bytes, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0
	}
	return bytes / (1024 * 1024 * 1024)
}

func detectNetworkIPs() []string {
	var ips []string
	ifaces, err := net.Interfaces()
	if err != nil {
		return ips
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ip, _, err := net.ParseCIDR(addr.String())
			if err != nil {
				continue
			}
			if ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			ips = append(ips, fmt.Sprintf("%s (%s)", ip.String(), iface.Name))
		}
	}
	return ips
}

func detectKernel() string {
	out, err := exec.Command("uname", "-r").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func min64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
