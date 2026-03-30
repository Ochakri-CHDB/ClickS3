package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	Endpoint     string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Profile      string
	Bucket       string
	Region       string
	Prefix       string

	Scenarios  []string
	Duration   time.Duration
	OutputFile string

	// Node role for distributed mode
	Role   string
	NodeID string

	// Concurrency settings (matching ClickHouse defaults)
	InsertThreads int
	MergeThreads  int
	SelectThreads int

	// ClickHouse S3 parameters
	MaxSinglePartUploadSize   int64 // 32 MB
	MinUploadPartSize         int64 // 16 MB
	MaxInflightParts          int   // 20
	UploadPartSizeMultFactor  int   // 2
	UploadPartSizeMultThresh  int   // 500
	MaxDownloadThreads        int   // 4
	MaxDownloadBufferSize     int64 // 10 MB (recommended: 50 MB)
	BackgroundPoolSize        int   // 64
	RequestTimeoutMs          int   // 30,000 ms
	ConnectTimeoutMs          int   // 10,000 ms
	MaxSingleReadRetries      int   // 4 (outer)
	MaxRetryAttempts          int   // 10 (inner SDK, effective 11)

	// Test intensity
	WarmupDuration time.Duration
	MaxConcurrency int // upper bound for IOPS adaptive ramp-up

	// Orchestrator mode: deploy + run from one node
	Nodes   []string // list of SSH targets (user@host or just host)
	SSHKey  string   // path to SSH private key
	SSHUser string   // default SSH user

	// TLS configuration for S3-like endpoints
	TLSCACert    string // path to custom CA certificate (PEM)
	TLSSkipVerify bool  // skip TLS certificate verification (self-signed)

	// Named test preset
	ScenarioPreset string

	// Quick measurement mode
	ReportSummary bool

	// Feature flags
	ShowVersion bool
	PathStyle   bool
	UseAWSChain bool
	NoCleanup   bool
	Verbose     bool
	AutoScale   bool

	// Detected server resources (populated at runtime)
	Resources *ServerResources
}

// ClickHouseRequirements are the minimum S3 capabilities required for ClickHouse
// to operate correctly. These are fixed constants derived from ClickHouse workload
// analysis (SharedMergeTree engine). They do not depend on test machine hardware.
type ClickHouseRequirements struct {
	// Latency — what ClickHouse can tolerate before retries and degradation
	// Source: CH request_timeout_ms=30000, inner retries=10, outer retries=4
	// At P99 > 1000ms, ClickHouse retry storms start. At P99 > 5000ms, inserts fail.
	GetRangeP99MaxMs       float64
	GetRangeP50MaxMs       float64
	GetFullP99MaxMs        float64
	PutSmallP99MaxMs       float64
	PutLargeP99MaxMs       float64
	UploadPartP99MaxMs     float64
	CompleteMultipartMaxMs float64
	DeleteObjectsP99MaxMs  float64
	ListObjectsP99MaxMs    float64

	// Absolute timeout — CH kills requests after 30s and retries
	// 4 outer x 11 inner x 30s = 23min retry cascade if hit repeatedly
	HardTimeoutMs int

	// IOPS — minimum to sustain ClickHouse 15M rows/min ingestion
	// Source: P99 measured from representative ClickHouse workloads
	MinGetIOPS  float64
	MinPutIOPS  float64
	MinHeadIOPS float64

	// Throughput — to avoid network becoming bottleneck
	MinGetMBps float64
	MinPutMBps float64

	// Concurrency — CH opens many parallel connections per node
	MinConcurrentConnections int

	// Consistency
	MaxConsistencyViolations int64
	MaxIdleConnectionResets  int
	IdleTimeoutTestSeconds  int
}

var CHRequirements = ClickHouseRequirements{
	GetRangeP99MaxMs:       200,
	GetRangeP50MaxMs:       50,
	GetFullP99MaxMs:        200,
	PutSmallP99MaxMs:       500,
	PutLargeP99MaxMs:       2000,
	UploadPartP99MaxMs:     2000,
	CompleteMultipartMaxMs: 500,
	DeleteObjectsP99MaxMs:  500,
	ListObjectsP99MaxMs:    500,

	HardTimeoutMs: 30000,

	MinGetIOPS:  4700,
	MinPutIOPS:  1100,
	MinHeadIOPS: 2000,

	MinGetMBps: 200,
	MinPutMBps: 100,

	MinConcurrentConnections: 1000,

	MaxConsistencyViolations: 0,
	MaxIdleConnectionResets:  0,
	IdleTimeoutTestSeconds:   61,
}

const (
	// ClickHouse: any request > 30s = timeout -> retry storm
	MaxAcceptableLatencyMs = 30000

	// ClickHouse: retry cascade worst case = 23 minutes
	// 4 outer x 11 inner x 30s = ~23 min
	OuterRetries = 4
	InnerRetries = 10
)

var allScenarios = []string{"compat", "insert", "merge", "select", "mixed", "failures", "iops"}

func ParseConfig() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.Endpoint, "endpoint", envOr("S3_ENDPOINT", ""), "S3 endpoint URL (e.g. https://minio:9000)")
	flag.StringVar(&cfg.AccessKey, "access-key", envOr("S3_ACCESS_KEY", ""), "S3 access key")
	flag.StringVar(&cfg.SecretKey, "secret-key", envOr("S3_SECRET_KEY", ""), "S3 secret key")
	flag.StringVar(&cfg.SessionToken, "session-token", envOr("S3_SESSION_TOKEN", ""), "S3 session token (for AWS STS)")
	flag.StringVar(&cfg.Profile, "profile", envOr("AWS_PROFILE", ""), "AWS profile name from ~/.aws/credentials")
	flag.StringVar(&cfg.Bucket, "bucket", envOr("S3_BUCKET", "clicks3-test"), "S3 bucket name")
	flag.StringVar(&cfg.Region, "region", envOr("S3_REGION", "us-east-1"), "S3 region")
	flag.StringVar(&cfg.Prefix, "prefix", "mergetree/", "Object key prefix (ClickHouse default: mergetree/)")
	flag.BoolVar(&cfg.UseAWSChain, "aws-chain", false, "Use default AWS credential chain (~/.aws/credentials)")

	var scenarios string
	flag.StringVar(&scenarios, "scenarios", "all", "Comma-separated: compat,insert,merge,select,mixed,failures,iops,all")
	flag.DurationVar(&cfg.Duration, "duration", 5*time.Minute, "Duration per scenario")
	flag.StringVar(&cfg.OutputFile, "output", "", "JSON report output file")

	flag.StringVar(&cfg.Role, "role", "standalone", "Node role: standalone (all on 1 node), node1/node2/node3 (distributed, all run mixed)")
	flag.StringVar(&cfg.NodeID, "node-id", envOr("CLICKS3_NODE_ID", ""), "Node identifier for distributed reports (default: hostname)")

	flag.IntVar(&cfg.InsertThreads, "insert-threads", 20, "Concurrent insert threads")
	flag.IntVar(&cfg.MergeThreads, "merge-threads", 8, "Concurrent merge threads")
	flag.IntVar(&cfg.SelectThreads, "select-threads", 50, "Concurrent select/query threads")

	// ClickHouse S3 parameters
	flag.IntVar(&cfg.RequestTimeoutMs, "request-timeout", 30000, "S3 request timeout in ms (CH default: 30000)")
	flag.IntVar(&cfg.ConnectTimeoutMs, "connect-timeout", 10000, "S3 connect timeout in ms (CH default: 10000)")
	flag.IntVar(&cfg.MaxSingleReadRetries, "read-retries", 4, "Outer read retries (CH: s3_max_single_read_retries=4)")
	flag.IntVar(&cfg.MaxRetryAttempts, "retry-attempts", 10, "Inner SDK retries (CH: s3_retry_attempts=10)")
	flag.IntVar(&cfg.MaxInflightParts, "max-inflight-parts", 20, "Max in-flight multipart parts (CH: 20)")
	flag.IntVar(&cfg.MaxDownloadThreads, "download-threads", 4, "Download threads per file (CH: max_download_threads=4)")

	flag.DurationVar(&cfg.WarmupDuration, "warmup", 30*time.Second, "Warmup duration before measurement")
	flag.IntVar(&cfg.MaxConcurrency, "max-concurrency", 2000, "Maximum concurrent S3 connections for IOPS ramp-up")

	var nodesStr string
	flag.StringVar(&nodesStr, "nodes", "", "Distributed mode: comma-separated SSH targets (e.g. 10.0.1.1,10.0.1.2,10.0.1.3)")
	flag.StringVar(&cfg.SSHKey, "ssh-key", envOr("CLICKS3_SSH_KEY", ""), "SSH private key path for distributed mode")
	flag.StringVar(&cfg.SSHUser, "ssh-user", envOr("CLICKS3_SSH_USER", "ec2-user"), "SSH user for distributed mode (default: ec2-user)")

	flag.StringVar(&cfg.TLSCACert, "tls-ca-cert", envOr("S3_TLS_CA_CERT", ""), "Path to custom CA certificate (PEM) for S3 TLS")
	flag.BoolVar(&cfg.TLSSkipVerify, "tls-skip-verify", false, "Skip TLS certificate verification (for self-signed certs)")

	flag.StringVar(&cfg.ScenarioPreset, "scenario-preset", "", "Named test preset: s3-baseline | minio-hdd (overrides thread/duration flags)")
	flag.BoolVar(&cfg.ReportSummary, "report-summary", false, "Run compat+iops only and print a compact shareable result block")

	flag.BoolVar(&cfg.ShowVersion, "version", false, "Print version and exit")
	flag.BoolVar(&cfg.AutoScale, "auto-scale", true, "Auto-scale threads based on server resources")
	flag.BoolVar(&cfg.PathStyle, "path-style", true, "Use path-style S3 addressing (required for MinIO)")
	flag.BoolVar(&cfg.NoCleanup, "no-cleanup", false, "Skip cleanup after test")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "Verbose output")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `ClickS3 — S3 Storage Capability Discovery for ClickHouse

Discovers the actual capabilities of any S3-compatible storage backend and
evaluates whether those capabilities meet ClickHouse's requirements.

Instead of checking against hardware-specific thresholds, ClickS3:
  1. Ramps concurrency until saturation (adaptive discovery)
  2. Reports measured peak IOPS, throughput, and latency
  3. Compares against fixed ClickHouse minimum requirements

Single-node mode (default):
  All scenarios run on one machine simulating 3 ClickHouse nodes.

Distributed mode (one command):
  Run from ANY machine — automatically deploys binary via SSH, runs all
  workloads on every node, collects and merges reports.

Usage:
  clicks3 [flags]

Examples:
  # Single node — run everything
  clicks3 --endpoint https://minio:9000 --access-key X --secret-key X

  # Distributed — ONE command deploys+runs on all nodes
  clicks3 --endpoint https://minio:9000 --access-key X --secret-key X \
    --nodes 10.0.1.1,10.0.1.2,10.0.1.3 --ssh-key ~/.ssh/id_rsa

  # AWS S3 with SSO profile
  clicks3 --aws-chain --profile MyProfile --region eu-west-1 --bucket my-bucket

  # Validation preset — cloud S3 baseline
  clicks3 --scenario-preset s3-baseline --aws-chain --region us-east-1 --bucket my-bucket

  # Validation preset — MinIO on HDD
  clicks3 --scenario-preset minio-hdd --endpoint https://minio:9000 ...

  # Quick measurement (~20 min, shareable result block)
  clicks3 --report-summary --endpoint https://minio:9000 --access-key X --secret-key X

  # IOPS discovery with custom ramp-up limit
  clicks3 --endpoint https://minio:9000 --max-concurrency 4000 ...

  # S3-like with TLS (custom CA)
  clicks3 --endpoint https://s3.internal:9000 --tls-ca-cert /path/to/ca.pem ...

  # S3-like with self-signed cert (skip verify)
  clicks3 --endpoint https://minio:9000 --tls-skip-verify ...

Environment variables:
  S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_SESSION_TOKEN,
  S3_BUCKET, S3_REGION, AWS_PROFILE, S3_TLS_CA_CERT,
  CLICKS3_SSH_KEY, CLICKS3_SSH_USER

Flags:
`)
		flag.PrintDefaults()
	}
	flag.Parse()

	// Apply scenario preset (only overrides flags not explicitly set by the user)
	if cfg.ScenarioPreset != "" {
		explicitFlags := make(map[string]bool)
		flag.Visit(func(f *flag.Flag) { explicitFlags[f.Name] = true })

		switch cfg.ScenarioPreset {
		case "s3-baseline":
			if !explicitFlags["duration"] {
				cfg.Duration = 10 * time.Minute
			}
			if !explicitFlags["warmup"] {
				cfg.WarmupDuration = 2 * time.Minute
			}
			if !explicitFlags["insert-threads"] {
				cfg.InsertThreads = 20
			}
			if !explicitFlags["merge-threads"] {
				cfg.MergeThreads = 8
			}
			if !explicitFlags["select-threads"] {
				cfg.SelectThreads = 50
			}
			if !explicitFlags["max-concurrency"] {
				cfg.MaxConcurrency = 2000
			}
			if !explicitFlags["scenarios"] {
				scenarios = "compat,iops,merge,mixed"
			}
		case "minio-hdd":
			if !explicitFlags["duration"] {
				cfg.Duration = 15 * time.Minute
			}
			if !explicitFlags["warmup"] {
				cfg.WarmupDuration = 3 * time.Minute
			}
			if !explicitFlags["insert-threads"] {
				cfg.InsertThreads = 48
			}
			if !explicitFlags["merge-threads"] {
				cfg.MergeThreads = 16
			}
			if !explicitFlags["select-threads"] {
				cfg.SelectThreads = 100
			}
			if !explicitFlags["max-concurrency"] {
				cfg.MaxConcurrency = 2000
			}
			if !explicitFlags["scenarios"] {
				scenarios = "compat,iops,merge,mixed"
			}
		default:
			fmt.Fprintf(os.Stderr, "Warning: unknown --scenario-preset %q (valid: s3-baseline, minio-hdd)\n", cfg.ScenarioPreset)
		}
	}

	// Apply --report-summary overrides (only for flags not explicitly set by user)
	if cfg.ReportSummary {
		explicitFlags := make(map[string]bool)
		flag.Visit(func(f *flag.Flag) { explicitFlags[f.Name] = true })

		if !explicitFlags["scenarios"] {
			scenarios = "compat,iops"
		}
		if !explicitFlags["duration"] {
			cfg.Duration = 5 * time.Minute
		}
		if !explicitFlags["warmup"] {
			cfg.WarmupDuration = 1 * time.Minute
		}
		if !explicitFlags["max-concurrency"] {
			cfg.MaxConcurrency = 1000
		}
		if !explicitFlags["insert-threads"] {
			cfg.InsertThreads = 20
		}
		if !explicitFlags["merge-threads"] {
			cfg.MergeThreads = 8
		}
		if !explicitFlags["select-threads"] {
			cfg.SelectThreads = 50
		}
	}

	// Parse distributed nodes list
	if nodesStr != "" {
		for _, n := range strings.Split(nodesStr, ",") {
			n = strings.TrimSpace(n)
			if n != "" {
				cfg.Nodes = append(cfg.Nodes, n)
			}
		}
	}

	// Apply defaults for ClickHouse S3 parameters
	if cfg.MaxSinglePartUploadSize == 0 {
		cfg.MaxSinglePartUploadSize = 32 * 1024 * 1024 // 32 MB
	}
	if cfg.MinUploadPartSize == 0 {
		cfg.MinUploadPartSize = 16 * 1024 * 1024 // 16 MB
	}
	if cfg.UploadPartSizeMultFactor == 0 {
		cfg.UploadPartSizeMultFactor = 2
	}
	if cfg.UploadPartSizeMultThresh == 0 {
		cfg.UploadPartSizeMultThresh = 500
	}
	if cfg.MaxDownloadBufferSize == 0 {
		cfg.MaxDownloadBufferSize = 10 * 1024 * 1024 // 10 MB default, 50 MB recommended
	}
	if cfg.BackgroundPoolSize == 0 {
		cfg.BackgroundPoolSize = 64
	}

	if scenarios == "all" {
		cfg.Scenarios = allScenarios
	} else {
		cfg.Scenarios = strings.Split(scenarios, ",")
	}

	// Distributed mode: if no explicit --scenarios, default to full suite
	if scenarios == "all" {
		switch cfg.Role {
		case "node1", "node2", "node3":
			cfg.Scenarios = allScenarios
		}
	}

	// Default node-id to hostname if not set
	if cfg.NodeID == "" {
		cfg.NodeID, _ = os.Hostname()
		if cfg.Role != "standalone" {
			cfg.NodeID = cfg.Role + "-" + cfg.NodeID
		}
	}

	// Detect server resources and auto-scale threads
	cfg.Resources = DetectResources()

	if cfg.AutoScale {
		cfg.InsertThreads = cfg.Resources.ScaleThreads(cfg.InsertThreads)
		cfg.MergeThreads = cfg.Resources.ScaleThreads(cfg.MergeThreads)
		cfg.SelectThreads = cfg.Resources.ScaleThreads(cfg.SelectThreads)
	}

	return cfg
}

func (c *Config) Validate() error {
	if c.Endpoint == "" && !c.UseAWSChain && c.Profile == "" {
		return fmt.Errorf("--endpoint or --aws-chain or --profile is required")
	}
	if !c.UseAWSChain && c.Profile == "" {
		if c.AccessKey == "" {
			return fmt.Errorf("--access-key is required (or use --aws-chain / --profile)")
		}
		if c.SecretKey == "" {
			return fmt.Errorf("--secret-key is required (or use --aws-chain / --profile)")
		}
	}
	if c.Bucket == "" {
		return fmt.Errorf("--bucket is required (or set S3_BUCKET)")
	}
	for _, s := range c.Scenarios {
		valid := false
		for _, a := range allScenarios {
			if s == a {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("unknown scenario: %q (valid: %s)", s, strings.Join(allScenarios, ","))
		}
	}
	if c.ScenarioPreset != "" && c.ScenarioPreset != "s3-baseline" && c.ScenarioPreset != "minio-hdd" {
		return fmt.Errorf("unknown --scenario-preset: %q (valid: s3-baseline, minio-hdd)", c.ScenarioPreset)
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
