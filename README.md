# ClickS3

**Can your object store handle ClickHouse?**

ClickS3 answers this question by replaying the *exact* S3 operations that ClickHouse generates, measuring what your storage can actually do, and telling you whether it's enough.

```
./clicks3 --endpoint https://minio:9000 --access-key X --secret-key X --bucket test
```

---

## Why Does This Tool Exist?

ClickHouse (SharedMergeTree engine) stores all its data on S3-compatible object storage. Every INSERT, every background merge, every SELECT query translates into dozens or hundreds of S3 API calls.

**The problem:** not all S3-compatible storage backends are created equal. Some are too slow. Some have API incompatibilities. Some reset connections. Some give stale reads. Any of these will cause ClickHouse to malfunction — silently or catastrophically.

**What ClickS3 does:**
1. Verifies that your storage supports every S3 API ClickHouse needs
2. Replays realistic ClickHouse workloads (INSERT, MERGE, SELECT) at scale
3. Discovers the peak IOPS and throughput your storage can sustain
4. Tells you how big a ClickHouse cluster this storage can support

**What it does NOT do:** it does not compare against hardcoded thresholds tied to one specific hardware. Instead, it *discovers* your storage's actual capabilities and evaluates them against what ClickHouse objectively requires.

---

## Table of Contents

- [How ClickHouse Uses S3](#how-clickhouse-uses-s3-the-essential-context)
- [The 7 Test Scenarios](#the-7-test-scenarios)
- [How Testing Works — Methodology](#how-testing-works--methodology)
- [ClickHouse Minimum Requirements](#clickhouse-minimum-requirements)
- [Capacity Planner — "How big can my cluster be?"](#capacity-planner--how-big-can-my-cluster-be)
- [Interpreting Results](#interpreting-results)
- [Validation Presets](#validation-presets)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
- [Project Structure](#project-structure)

---

## How ClickHouse Uses S3 — The Essential Context

Before understanding what ClickS3 tests, you need to understand *how* ClickHouse talks to S3. Everything ClickS3 does is a faithful reproduction of these patterns.

### Writing data (INSERT)

When data arrives, ClickHouse packages it into a **data part** — a group of 6 to 9 files written to S3 in parallel:

```
data.bin            ← the actual column data (can be 100 KB to 200 MB)
data.mrk3           ← mark file (index into data.bin, ~500 KB)
primary.idx         ← primary key index (~48 KB)
columns.txt         ← column list (288 bytes)
checksums.txt       ← integrity checksums (96 bytes)
count.txt           ← row count (4 bytes)
partition.dat       ← partition key (4 bytes)
minmax_col.idx      ← MinMax index (8–32 bytes)
```

Small parts (< 32 MB) use a single `PutObject`. Larger parts use **multipart upload** with 16 MB starting chunk size, doubling the chunk size every 500 parts (16 → 32 → 64 MB). Note: in the current implementation, parts are uploaded sequentially per file.

**Why this matters for testing:** your storage must handle tiny files (4 bytes) and large multipart uploads (200+ MB) through the same API, at the same time, without errors.

### Background compaction (MERGE)

ClickHouse continuously merges small parts into larger ones (typically 4 parts → 1 merged part). Each merge cycle generates approximately **96–160 S3 operations** depending on part sizes:

1. **Read phase**: for each file in each source part (4 parts × 9 files), either GetObject (small files ≤ 64 KB) or multiple GetObjectRange calls (64 KB each, up to 20 ranges for large files)
2. **Write phase** (9 ops): PutObject for each file in the merged part (uses multipart for files > 32 MB)
3. **Delete phase** (1 op): one DeleteObjects batch call with all source file keys (typically 36 keys)

**Why this matters for testing:** merges run continuously in the background. If your storage can't sustain this I/O rate, parts accumulate faster than they merge, eventually triggering the dreaded `TOO_MANY_PARTS` error.

### Reading data (SELECT)

Each query reads data in **64 KB chunks** called "granules" (8,192 rows each). A typical query touching 3 data parts generates approximately **280 GET operations**:

```
3 × HeadObject                → check parts exist
3 × GetObject(primary.idx)    → read primary keys to find relevant granules
3 × GetObject(data.mrk3)      → read mark files to find byte offsets
3 × 90 × GetObjectRange(64KB) → read ~90 granules from each part
```

ClickHouse reads 4 granules in parallel per file (`max_download_threads=4`).

**Why this matters for testing:** query latency = (number of granules × GET latency). If your GET P99 is 200 ms and a query reads 270 granules, worst case is **54 seconds** for a single query.

### The 30-second cliff

ClickHouse has a hard request timeout of **30 seconds**. Any S3 request that takes longer triggers a retry cascade: 4 outer retries × 11 inner retries × 30 seconds = up to **23 minutes** of retries for a single failed request. This is why latency matters so much — one slow request can cascade into minutes of degradation.

---

## The 7 Test Scenarios

ClickS3 runs these scenarios in sequence. Each tests a different aspect of your storage.

### 1. `compat` — Can ClickHouse talk to your storage at all?

**The question:** does your storage implement every S3 API that ClickHouse needs?

**Why test this first:** if a mandatory API is missing or broken, nothing else matters. ClickS3 runs this scenario first and **aborts immediately** if any critical check fails.

**What it does:** 28 individual checks organized by severity:

| Level | Meaning | Count | If it fails... |
|-------|---------|-------|----------------|
| **MUST** | Mandatory | 17 | ClickHouse cannot function. Test aborts. |
| **SHOULD** | Strongly recommended | 8 | ClickHouse works but with degraded performance. |
| **OPTIONAL** | Nice to have | 3 | Minor feature missing, no real impact. |

**The 17 MUST checks test:**

- **Basic operations**: PutObject, GetObject (full + range), HeadObject, DeleteObjects (batch), ListObjectsV2
- **Multipart uploads**: CreateMultipartUpload, UploadPart, UploadPart with variable sizes (16→32→64 MB), CompleteMultipartUpload, AbortMultipartUpload
- **Data consistency**: read-after-write (PUT then immediate GET must return identical data), ETag coherence (part ETags must match during Complete), immediate 404 after Delete, concurrent read-after-write (20 goroutines, 5 rounds each, 0 violations tolerated)
- **Network behavior**: HTTP Keep-Alive (no connection resets), 1,000+ concurrent TCP connections, idle connection survival ≥ 60 seconds

**The 8 SHOULD checks test:** TTFB latency (64 KB GET P99), DNS rotation (multiple IPs), DeleteObject (single), ListMultipartUploads, parallel range reads, PUT various sizes (4 B to 32 MB), high concurrency (100 parallel ops), Content-Length header.

### 2. `insert` — How fast can your storage absorb writes?

**The question:** can your storage sustain ClickHouse's INSERT pressure without timeouts?

**What it does:**
- Writer threads: 20 by default (×3 in standalone mode = 60), auto-scaled based on server resources
- Each thread continuously writes data parts to S3 using the exact ClickHouse pattern (metadata files + data.bin)
- Workload mix: 40% small PUTs (< 5 MB), 40% medium (5–30 MB), 20% large (> 32 MB, using multipart)
- Runs for the full test duration (default 5 minutes) after a warmup phase

**What it checks:**
- PUT P99 latency < 500 ms (ClickHouse timeout budget)
- UploadPart P99 latency < 2,000 ms
- CompleteMultipart P99 latency < 500 ms
- Error rate < 0.01%
- Zero requests exceeding 30 seconds

### 3. `merge` — Can background compaction keep up?

**The question:** can your storage handle the continuous read-then-write-then-delete cycle of ClickHouse merges?

**What it does:**
- Merge threads: 8 by default, auto-scaled based on server resources
- Each thread simulates one merge cycle: read 4 source parts (GetObject for small files, GetObjectRange 64 KB for large files up to 20 ranges each), write 1 merged part (9 PutObject calls), delete source parts (one DeleteObjects batch)
- Verifies data consistency: after deleting source parts, a HeadObject on a deleted key must return 404 (not stale data)

**What it checks:**
- GET range P50 < 50 ms, P99 < 200 ms (merge reads dominate total I/O)
- DeleteObjects P99 < 500 ms
- Zero stale reads after delete (any = data corruption risk)
- Zero requests exceeding 30 seconds

### 4. `select` — How fast can queries read data?

**The question:** can your storage serve hundreds of 64 KB range GETs in parallel with low latency?

**What it does:**
- Query threads: 50 by default, auto-scaled based on server resources
- Each "query" reads 3 parts × (1 primary.idx + 1 marks + 30 × 64 KB range GETs on data.bin) = 96 GET operations
- Includes cache-cold (60% random part) and cache-hot (40% same part) access patterns

**What it checks:**
- GET range P50 < 50 ms (granule reads are the hot path)
- GET range P99 < 200 ms
- GET full P99 < 200 ms (index + marks files)
- Zero requests exceeding 30 seconds

### 5. `mixed` — The real test: everything at once

**The question:** what happens when INSERT, MERGE, and SELECT all compete for the same storage, just like real-world usage?

This is the most important scenario because ClickHouse runs all three workloads simultaneously on every node.

**What it does:**
- Every test node runs all 3 workloads at the same time
- Thread allocation: **30% INSERT, 15% MERGE, 55% SELECT** (realistic workload ratio)
- Inserts create new parts → merges compact them → selects read them — all concurrently, all hitting the same bucket and prefix
- Runs read-after-write consistency checks while everything else is happening

**What it checks:**
- Read-after-write consistency under load (0 violations)
- PUT throughput ≥ 100 MB/s
- GET throughput ≥ 200 MB/s
- Zero 503/SlowDown errors
- Zero requests exceeding 30 seconds

### 6. `failures` — Edge cases that break things

**The question:** does your storage handle uncommon-but-critical S3 behaviors correctly?

**What it does:**
- **Abort cleanup:** Initiate 50 multipart uploads, abort them all, verify zero orphaned uploads remain
- **Variable part sizes:** Upload with 16→32→64 MB progression (non-monotonic — ClickHouse does this)
- **Extreme concurrency:** Fire 500–2,000 PUTs simultaneously
- **Batch delete at scale:** Create 1,000 objects, delete them all in one `DeleteObjects` call
- **Consistency under stress:** 1,000 rounds of PUT→immediate GET→compare bytes

### 7. `iops` — How much can your storage actually do?

**The question:** what is the absolute maximum IOPS and throughput your storage can sustain, and what size ClickHouse cluster can it support?

This scenario has three phases:

#### Phase 1: Network bandwidth probe

Uploads then downloads a 256 MB object to measure raw network throughput to the storage endpoint. This determines whether the network itself will be the bottleneck.

#### Phase 2: Adaptive IOPS ramp-up

For two profiles (raw 4 KB objects and realistic ClickHouse sizes — 1 MB PUT / 64 KB GET), ClickS3 discovers the peak IOPS using adaptive concurrency ramp-up:

1. Start at 5 concurrent goroutines
2. Run for 30 seconds, measure IOPS + latency
3. Double concurrency: 5 → 10 → 20 → 40 → 80 → 160 → ...
4. Stop when saturation is detected:
   - IOPS growth drops below 5% (storage is maxed out)
   - P99 latency exceeds 3× baseline (degradation)
   - Error rate exceeds 5% (overload)
5. Report the peak IOPS before degradation

The output is a **latency curve** showing exactly where your storage saturates:

```
  PUT latency curve:
       5 thr │      312 IOPS │ P50=  15.2ms │ P99=  42.1ms │ err=0.0% │ ██░░░░░░░░░░░░░░░░░░░░░░
      10 thr │      608 IOPS │ P50=  15.8ms │ P99=  48.3ms │ err=0.0% │ ████░░░░░░░░░░░░░░░░░░░░
      20 thr │     1142 IOPS │ P50=  16.4ms │ P99=  55.7ms │ err=0.0% │ ████████░░░░░░░░░░░░░░░░
      40 thr │     1987 IOPS │ P50=  18.9ms │ P99=  72.4ms │ err=0.0% │ ██████████████░░░░░░░░░░
      80 thr │     3210 IOPS │ P50=  23.1ms │ P99= 110.5ms │ err=0.0% │ ██████████████████████░░
     160 thr │     4102 IOPS │ P50=  35.7ms │ P99= 189.3ms │ err=0.1% │ ████████████████████████  ← saturation
```

This ramp-up is performed separately for PUT, GET, Mixed (50/50), and HEAD operations.

#### Phase 3: Capacity planning

After measuring, the tool computes which ClickHouse cluster configurations your storage can support. See the [Capacity Planner](#capacity-planner--how-big-can-my-cluster-be) section below.

---

## How Testing Works — Methodology

### Thread scaling

ClickS3 auto-detects your machine's CPU and RAM and adjusts thread counts proportionally. A machine with 16 vCPU / 32 GB RAM will run ~2× the threads of an 8 vCPU / 16 GB machine. This ensures the test stresses your *storage*, not your *test machine*.

Thread scaling only affects concurrency (number of parallel operations). It does not affect IOPS targets or latency thresholds — those are fixed constants based on what ClickHouse needs.

### Warmup phase

Each scenario begins with a warmup phase (default 30 seconds) where ClickS3 runs the workload at reduced intensity. Measurements collected during warmup are discarded. This ensures results reflect steady-state performance, not cold-start effects.

### Object key pattern

All objects use random 32-character lowercase keys — the same pattern ClickHouse's SharedMergeTree uses. This prevents hash-ring hotspots on MinIO and ensures even distribution across storage backends.

### Cleanup

After each scenario, ClickS3 deletes all objects it created (unless `--no-cleanup` is set). A final cleanup pass runs at the end.

### Maximum concurrency

The IOPS ramp-up caps at `--max-concurrency` (default 2,000). This is an absolute cap, not scaled by CPU. It prevents the test from overwhelming a shared storage backend.

---

## ClickHouse Minimum Requirements

These are **fixed constants** based on what ClickHouse needs to function. They don't depend on your hardware.

### Latency

| Operation | P99 Max | Why this threshold |
|-----------|---------|-----|
| GET range (64 KB) | **200 ms** | Queries read ~270 granules — each adds up |
| GET full (primary.idx, marks) | **200 ms** | Read once per data part per query |
| PUT small (metadata) | **500 ms** | Many metadata files per insert |
| PUT large (multipart data) | **2,000 ms** | Large sequential writes, more tolerant |
| UploadPart | **2,000 ms** | Same budget as large PUT |
| CompleteMultipartUpload | **500 ms** | One call per large insert |
| DeleteObjects (batch) | **500 ms** | Merge cleanup |
| ListObjectsV2 | **500 ms** | Startup, not hot path |

**Hard timeout:** 30,000 ms — any request slower than 30 seconds triggers the retry cascade described above.

### IOPS

| Metric | Minimum | Source |
|--------|---------|--------|
| GET IOPS | **4,700** | Measured P99 from representative ClickHouse workloads |
| PUT IOPS | **1,100** | Same measurement source |
| HEAD IOPS | **2,000** | Part discovery operations |

### Throughput

| Metric | Minimum |
|--------|---------|
| GET throughput | **200 MB/s** |
| PUT throughput | **100 MB/s** |

### Consistency & Connections

| Metric | Requirement | Why |
|--------|-------------|-----|
| Read-after-write consistency violations | **0** | Stale reads = corrupted query results |
| Idle connection resets (after 60s) | **0** | Resets force TLS renegotiation overhead |
| Concurrent connections per node | **≥ 1,000** | ClickHouse default connection pool |

---

## Capacity Planner — "How big can my cluster be?"

After measuring your storage's peak capabilities, ClickS3 answers the inverse question: *"Given what this storage can do, what ClickHouse configurations can it support?"*

### The model

ClickHouse S3 I/O per replica depends primarily on **RAM per replica**. More RAM means larger buffers, larger data parts, more concurrent merges — and therefore more S3 operations.

The capacity planner uses **measured P90 IOPS per replica** from representative ClickHouse workloads (3-replica services, ≥ 60% memory utilization, ≥ 200 hours of data per entry):

| RAM/replica | P90 GET IOPS | P90 PUT IOPS | Data confidence |
|:-----------:|:------------:|:------------:|:---------------:|
| 8 GiB | 49 | 14 | 95,000 hours |
| 16 GiB | 116 | 27 | 72,000 hours |
| 32 GiB | 106 | 26 | 64,000 hours |
| 64 GiB | 200 | 61 | 50,000 hours |
| 120 GiB | 148 | 46 | 46,000 hours |
| 160 GiB | 298 | 143 | 6,000 hours |
| 236 GiB | 121 | 30 | 13,000 hours |
| 340 GiB | 83 | 19 | 624 hours † |
| 360 GiB | 115 | 45 | 1,347 hours † |

† Low confidence — less than 1,000 hours of measured data. Extrapolate with caution.

**Why P90 and not P99?** P99 is dominated by burst outliers (query spikes, backup jobs, schema migrations). P90 represents *sustained load* — what the storage must handle continuously. For planning, we multiply P90 by **2.5×** as a safety margin to account for peaks.

**Why not a linear model?** The relationship between RAM and IOPS is **not linear**. Larger nodes have proportionally more filesystem cache, which absorbs reads. The per-GiB IOPS coefficient varies significantly across RAM sizes. Using a lookup table with interpolation is more accurate than a single linear coefficient.

For RAM sizes not in the table (e.g., 48, 60, 96 GiB), the planner interpolates linearly between the two nearest entries. For RAM sizes above 360 GiB (e.g., 480, 720 GiB), it uses the 360 GiB values — no extrapolation beyond measured data.

### What the planner evaluates

For each combination of RAM size (13 sizes from 8 to 720 GiB) × replica count (1 to 20), the planner computes:

```
demand_get_iops = replicas × P90_get_per_replica × 2.5
demand_put_iops = replicas × P90_put_per_replica × 2.5
demand_get_mbps = demand_get_iops × 64 KB / 1024
demand_put_mbps = demand_put_iops × 1 MB
```

Then checks 5 constraints:
1. **GET IOPS**: is demand < measured peak GET IOPS?
2. **PUT IOPS**: is demand < measured peak PUT IOPS?
3. **GET bandwidth**: is demand < measured peak GET MB/s?
4. **PUT bandwidth**: is demand < measured peak PUT MB/s?
5. **Network**: is total bandwidth demand < measured network bandwidth?

A configuration is **supported** if all constraints pass.

### Output

The planner always prints a **max replicas summary** — one line per RAM size showing the largest cluster this storage can handle:

```
  Maximum supported replicas by RAM size:
  ┌──────────────┬───────────────┬────────────────────┐
  │ RAM/replica  │ Max replicas  │ Bottleneck         │
  ├──────────────┼───────────────┼────────────────────┤
  │     8 GiB    │      15       │  GET_IOPS at 16+   │
  │    16 GiB    │      10       │  GET_IOPS at 11+   │
  │    32 GiB    │       8       │  GET_IOPS at 9+    │
  │    64 GiB    │       5       │  GET_IOPS at 6+    │
  │   120 GiB    │       7       │  GET_IOPS at 8+    │
  │   236 GiB    │      12       │  GET_IOPS at 13+   │
  │   360 GiB    │       6 †     │  PUT_IOPS at 7+    │
  └──────────────┴───────────────┴────────────────────┘
    † = low confidence (<1000 measured hours)
```

In verbose mode (`--verbose`) or when a validation preset is active, it also prints the **full matrix table** (13 × 10 = 130 cells) showing GET demand, PUT demand, and verdict for every configuration:

```
  ┌──────────┬──────────────────────────────────────────────────────────────────┐
  │ RAM/rep  │  Replicas:     1     2     3     4     6     8    10    12    15 │
  ├──────────┼──────────────────────────────────────────────────────────────────┤
  │    8 GiB │  GET dem    123   245   368   490   735   980  1225  1470  1838 │
  │          │  PUT dem     35    70   105   140   210   280   350   420   525 │
  │          │  Verdict     OK    OK    OK    OK    OK    OK    OK    OK    ok │
  ├──────────┼──────────────────────────────────────────────────────────────────┤
  │   64 GiB │  GET dem    500  1000  1500  2000  3000  4000  5000  6000  7500 │
  │          │  ...                                                             │
  └──────────┴──────────────────────────────────────────────────────────────────┘
    OK = supported (>20% headroom)  ok = tight (<20% headroom)  NO:GT = GET IOPS limit
```

This directly answers: *"For my storage, what's the biggest ClickHouse cluster I can run?"*

---

## Interpreting Results

### Storage Capability Profile

After the IOPS scenario, ClickS3 prints a table comparing your storage's measured capabilities against ClickHouse's minimum requirements:

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │  Storage Capability Profile                                         │
  ├──────────────────────────────┬───────────────┬─────────────────────┤
  │  Metric                      │  Measured     │  CH Requires        │
  ├──────────────────────────────┼───────────────┼─────────────────────┤
  │  Peak GET IOPS (64KB)        │    8 432 ✓    │  >= 4700            │
  │  Peak PUT IOPS (1MB)         │      612 ✗    │  >= 1100            │
  │  Peak HEAD IOPS              │   14 200 ✓    │  >= 2000            │
  │  Peak GET throughput         │  523 MB/s ✓   │  >= 200 MB/s        │
  │  Peak PUT throughput         │  612 MB/s ✓   │  >= 100 MB/s        │
  │  GET P99 @ operating load    │    8.3 ms ✓   │  < 200 ms           │
  │  PUT P99 @ operating load    │  142.0 ms ✓   │  < 500 ms           │
  └─────────────────────────────────────────────────────────────────────┘
```

### What each failure means for ClickHouse

| Condition | What happens in practice |
|-----------|---------------------------|
| **GET IOPS < 4,700** | Merges slow down → parts accumulate → `TOO_MANY_PARTS` error → queries degrade |
| **PUT IOPS < 1,100** | Insert flushes block → async insert queue fills RAM → OOM risk |
| **PUT P99 > 2,000 ms** | Insert latency spikes → backpressure → client timeouts |
| **GET P99 > 200 ms** | Query latency = granules × latency (270 × 200 ms = 54s per query) |
| **Consistency violations > 0** | Stale reads after delete → corrupted query results, silent data loss |
| **Connection resets after 60s** | TLS handshake overhead on every request → latency spike every 60s |

### Final verdict

ClickS3 computes an overall verdict:

| Verdict | Meaning |
|---------|---------|
| **PASS** | All checks pass. This storage is suitable for ClickHouse. |
| **WARN** | ≥ 80% of checks pass. Mostly OK but review the failures. |
| **FAIL** | < 80% of checks pass. This storage is not suitable for ClickHouse as configured. |

---

## Validation Presets

Presets are named configurations that set optimal test parameters for common storage backends and print a validation guide comparing results against known-good reference ranges.

### `s3-baseline` — Cloud Object Storage

For AWS S3, GCS, Azure Blob, or any cloud object store.

```bash
clicks3 --scenario-preset s3-baseline --aws-chain --region us-east-1 --bucket my-bucket
```

Sets: 10 min duration, 2 min warmup, 20/8/50 threads, scenarios: compat + iops + merge + mixed.

Expected ranges: GET IOPS ≥ 50,000 | PUT IOPS ≥ 3,500 | GET P99: 80–300 ms | PUT P99: 150–800 ms.

### `minio-hdd` — Local MinIO on Spinning Disks

For MinIO clusters backed by HDD storage over LAN.

```bash
clicks3 --scenario-preset minio-hdd --endpoint https://minio:9000 \
        --access-key KEY --secret-key SECRET --bucket ch-bench
```

Sets: 15 min duration, 3 min warmup, 48/16/100 threads, scenarios: compat + iops + merge + mixed.

Expected ranges: GET IOPS (raw 4 KB): 15,000–21,600 | GET IOPS (64 KB): 3,000–8,000 | PUT IOPS (1 MB): 300–800 | GET P99: 2–20 ms.

### Override behavior

Explicit CLI flags always take precedence over preset values:

```bash
# Uses minio-hdd preset but overrides duration to 5 minutes
clicks3 --scenario-preset minio-hdd --duration 5m ...
```

### Validation guide

When a preset is active, ClickS3 prints a validation guide after the final verdict, comparing each measured metric against the preset's reference ranges. This output is also included in the JSON report under `validation_guide`.

---

## Quick Measurement (20 minutes, shareable result)

The `--report-summary` flag runs only the two most relevant scenarios (compat + iops) and prints a compact, copy-pasteable result block at the end. This is not a full benchmark — it is a quick measurement run designed to produce a result block that can be shared for analysis.

### Test 1 — AWS S3

```bash
clicks3 --report-summary \
        --aws-chain --region us-east-1 \
        --bucket clicks3-test-$(date +%s)
```

### Test 2 — MinIO

```bash
clicks3 --report-summary \
        --endpoint https://YOUR_MINIO_OR_HAPROXY:9000 \
        --access-key YOUR_KEY \
        --secret-key YOUR_SECRET \
        --path-style \
        --tls-skip-verify \
        --bucket clicks3-test
```

Run both tests and share the **RESULT BLOCK** from each run for analysis.

The result block looks like this:

```
================================================================
  RESULT BLOCK -- copy everything between the lines and share it
================================================================
endpoint:       https://minio:9000
region:         us-east-1
path_style:     true
test_machine:   bench-01  16 vCPU  32 GB RAM
scale_factor:   2.00

--- compat ---
must:           17/17
should:         8/8
ttfb_p50_ms:    3.2
ttfb_p99_ms:    12.4

--- iops ---
peak_get_iops_raw:      18432
peak_put_iops_raw:      4210
peak_get_iops_ch:       8432
peak_put_iops_ch:       612
peak_get_mbps:          523
peak_put_mbps:          612
get_p99_at_peak_ms:     8.3
put_p99_at_peak_ms:     142.0
get_saturation_threads: 160
put_saturation_threads: 80
network_bw_mbps:        950
consistency_violations: 0

================================================================
```

When `--output report.json` is specified, the result block is also written to the JSON file under the `"summary_block"` field.

---

## Quick Start

### Build

```bash
git clone https://github.com/ClickHouse/ClickS3.git
cd ClickS3 && go build -o clicks3 .
```

### Run

```bash
# Against MinIO or any S3-compatible endpoint
./clicks3 --endpoint https://minio:9000 --access-key X --secret-key X --bucket test

# Against AWS S3 with SSO
./clicks3 --aws-chain --profile MyProfile --region eu-west-1 --bucket my-bucket

# TLS with custom CA certificate
./clicks3 --endpoint https://s3.internal:9000 --tls-ca-cert /path/to/ca.pem \
  --access-key KEY --secret-key SECRET

# Compatibility check only (fast, ~2 minutes)
./clicks3 ... --scenarios compat

# IOPS discovery only (with capacity planner)
./clicks3 ... --scenarios iops --verbose

# Full suite with JSON export
./clicks3 ... --output report.json
```

### Distributed mode

Run from one machine — ClickS3 deploys itself to all nodes via SSH, runs the benchmark on every node in parallel, collects reports, and merges them:

```bash
./clicks3 --endpoint https://minio:9000 --access-key X --secret-key X \
  --nodes 10.0.1.1,10.0.1.2,10.0.1.3 \
  --ssh-key ~/.ssh/id_rsa --ssh-user ec2-user
```

---

## CLI Reference

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoint` | | S3 endpoint URL (e.g. `https://minio:9000`) |
| `--access-key` | | S3 access key |
| `--secret-key` | | S3 secret key |
| `--bucket` | `clicks3-test` | Bucket name |
| `--region` | `us-east-1` | AWS region |
| `--aws-chain` | `false` | Use default AWS credential chain |
| `--profile` | | AWS profile name |
| `--scenarios` | `all` | Comma-separated: compat, insert, merge, select, mixed, failures, iops |
| `--duration` | `5m` | Duration per scenario |
| `--warmup` | `30s` | Warmup duration before measurement |
| `--max-concurrency` | `2000` | Max concurrent connections for IOPS ramp-up |
| `--scenario-preset` | | Named preset: `s3-baseline` or `minio-hdd` |
| `--report-summary` | `false` | Run compat+iops only, print a compact shareable result block |
| `--output` | | JSON report output file |
| `--insert-threads` | `20` | INSERT concurrency |
| `--merge-threads` | `8` | MERGE concurrency |
| `--select-threads` | `50` | SELECT concurrency |
| `--nodes` | | Distributed mode: comma-separated SSH targets |
| `--ssh-key` | | SSH private key path |
| `--ssh-user` | `ec2-user` | SSH user |
| `--tls-ca-cert` | | Custom CA certificate (PEM) |
| `--tls-skip-verify` | `false` | Skip TLS verification (self-signed certs) |
| `--path-style` | `true` | Path-style S3 addressing (required for MinIO) |
| `--auto-scale` | `true` | Auto-scale threads based on server resources |
| `--no-cleanup` | `false` | Skip cleanup after test |
| `--verbose` | `false` | Verbose output (includes full capacity matrix) |
| `--version` | | Print version and exit |

### Environment Variables

| Variable | Equivalent flag |
|----------|----------------|
| `S3_ENDPOINT` | `--endpoint` |
| `S3_ACCESS_KEY` | `--access-key` |
| `S3_SECRET_KEY` | `--secret-key` |
| `S3_BUCKET` | `--bucket` |
| `S3_REGION` | `--region` |
| `AWS_PROFILE` | `--profile` |
| `S3_TLS_CA_CERT` | `--tls-ca-cert` |

---

## Project Structure

```
clicks3/
├── main.go                  Entry point
├── config.go                CLI flags, ClickHouse minimum requirements (CHRequirements)
├── sysinfo.go               Hardware detection, thread scaling
├── s3ops.go                 S3 client (ClickHouse timeout/retry parameters)
├── keygen.go                Random key generator (32-char ClickHouse pattern)
├── metrics.go               Latency/throughput collector (P50/P90/P95/P99)
├── runner.go                Scenario orchestrator (run order, abort logic)
├── report.go                Terminal + JSON reports, validation guide
├── orchestrator.go          Distributed SSH deployment
├── capacity_planner.go      Capacity planner (P90 lookup table, interpolation)
├── scenario_compat.go       S3 API compatibility (28 MUST/SHOULD/OPTIONAL checks)
├── scenario_insert.go       INSERT workload simulation
├── scenario_merge.go        MERGE (background compaction) simulation
├── scenario_select.go       SELECT (OLAP query) simulation
├── scenario_mixed.go        All workloads simultaneously
├── scenario_failures.go     Edge cases + consistency stress tests
├── scenario_iops.go         IOPS discovery (adaptive ramp-up + network probe)
└── version.go               Version string
```

## License

Apache License 2.0
