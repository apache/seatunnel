# StainTrace - Quick Start Guide

## Overview

StainTrace is SeaTunnel's **data lineage and end-to-end performance tracing system** for tracking the complete data flow within the engine.

### Core Features

- **Framework-level Implementation**: Works with all Connectors out-of-the-box, no connector code changes needed
- **6 Basic Stages**: S0 → Q+ → Q- → T+ → T- → W! (complete end-to-end pipeline, all instrumented)
- **Extended Fine-grained Stages**: 40+ stage codes defined for future instrumentation (see stage tables below)
- **Local File Storage**: Zero dependencies, JSON Lines format, lightweight
- **Task-level Control**: Engine-level + task-level dual switches for flexible sampling control
- **Offline Analysis Tool**: Standalone analyzer generates HTML reports
- **OpenTelemetry Integration**: Native support for OTel Span JSON format
- **Performance Optimized**: Sampling rate control + batch processing + rate limiting, overhead <2%

### 6 Basic Tracing Stages

1. **S0** (SOURCE_EMIT): Source emits data
2. **Q+** (QUEUE_IN): Enter queue
3. **Q-** (QUEUE_OUT): Leave queue
4. **T+** (TRANSFORM_IN): Transform receives data
5. **T-** (TRANSFORM_OUT): Transform outputs data
6. **W!** (SINK_WRITE_DONE): Sink write completed

### Stage Details

#### Basic Stages (1-6)
| Stage | Code | Description | Recording Location |
|-------|------|-------------|-------------------|
| SOURCE_EMIT | 1 | Source emits data | SeaTunnelSourceCollector.collect() |
| QUEUE_IN | 2 | Enter queue (before enqueue, captures backpressure) | IntermediateQueue.received() |
| QUEUE_OUT | 3 | Leave queue (after dequeue) | IntermediateQueue.collect() |
| TRANSFORM_IN | 4 | Transform receives data | TransformFlowLifeCycle.received() |
| TRANSFORM_OUT | 5 | Transform outputs data | TransformFlowLifeCycle before output |
| SINK_WRITE_DONE | 6 | Sink write completed | After SinkFlowLifeCycle.writer.write() |

#### Key Performance Stages (101-110)
> ⚠️ **Planned — not yet instrumented.** These stage codes are defined in `StainTraceStage` but no production call site exists yet. They will not appear in trace files until instrumented.

| Stage | Code | Description | Purpose |
|-------|------|-------------|---------|
| SOURCE_READ_END | 101 | Source read completed | Source read performance |
| QUEUE_OFFER_START | 102 | Queue enqueue started | Backpressure detection |
| TRANSFORM_EXECUTE_START | 104 | Transform execution started | Transform performance analysis |
| TRANSFORM_EXECUTE_END | 105 | Transform execution ended | Transform performance analysis |
| SINK_BATCH_AGGREGATE_END | 106 | Sink batch aggregation completed | Sink batch processing performance |
| SINK_FORMAT_END | 107 | Sink formatting completed | Data formatting performance |
| SINK_WRITE_START | 108 | Sink I/O write started | I/O performance analysis |
| SINK_WRITE_END | 109 | Sink I/O write ended | I/O performance analysis |
| SINK_COMMIT_END | 110 | Sink commit completed | Transaction commit performance |

#### Extended Fine-grained Stages (201-220)
> ⚠️ **Planned — not yet instrumented.**

| Stage | Code | Description | Purpose |
|-------|------|-------------|---------|
| SOURCE_READ_START | 201 | Source read started | Source performance |
| SOURCE_SERIALIZE_START/END | 202-203 | Source serialization | Serialization performance |
| TRANSFORM_PARSE_START/END | 205-206 | Transform parsing | Parsing performance |
| TRANSFORM_BUILD_START/END | 207-208 | Transform result building | Building performance |
| SINK_RECEIVE | 209 | Sink receives data | Data flow tracking |
| SINK_BATCH_AGGREGATE_START | 210 | Sink batch aggregation started | Batch processing performance |
| SINK_FORMAT_START | 211 | Sink formatting started | Formatting performance |
| SINK_COMMIT_START | 212 | Sink commit started | Commit performance |
| CHECKPOINT_SNAPSHOT_START/END | 213-214 | Checkpoint snapshot | Checkpoint performance |
| CHECKPOINT_BARRIER_EMIT/RECEIVE | 215-216 | Checkpoint Barrier | Barrier propagation |

#### Network Transfer Stages (217-220, Multi-node Cluster Only)
> ⚠️ **Planned — not yet instrumented.** Requires hooking into the Hazelcast serialization layer for cross-node transfers.

| Stage | Code | Description | When It Appears |
|-------|------|-------------|-----------------|
| RECORD_SERIALIZE_START | 217 | Data serialization started | Multi-node cluster, cross-node data transfer |
| RECORD_SERIALIZE_END | 218 | Data serialization ended | Same as above |
| RECORD_DESERIALIZE_START | 219 | Data deserialization started | Same as above |
| RECORD_DESERIALIZE_END | 220 | Data deserialization ended | Same as above |

#### Flow Control Audit Stages (226-227)
> ⚠️ **Planned — not yet instrumented.**

| Stage | Code | Description | Purpose |
|-------|------|-------------|---------|
| FLOW_CONTROL_AUDIT_START | 226 | Flow control audit started | Backpressure detection |
| FLOW_CONTROL_AUDIT_END | 227 | Flow control audit ended | Backpressure detection |

> **⚠️ Important Notes**:
> - **Single-node Execution**: Data transfers via in-memory queues, serialization stages (217-220) **will NOT appear**
> - **Multi-node Cluster**: Serialization stages **will appear** when data needs network transfer between nodes
> - Serialization duration can be calculated via `gap_ms`: `RECORD_SERIALIZE_END - RECORD_SERIALIZE_START`

### Local File Storage

StainTrace uses local file storage for trace data:

- **Zero Dependencies**: No database or external services required
- **Lightweight**: JSON Lines format, human-readable
- **Offline Analysis**: Standalone analyzer tool generates HTML reports
- **Storage Path**: Configured by `stain-trace-file-base-path` (no default). For example:
  `/tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/`

---

## Quick Start

### Step 1: Configure Engine

Edit `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
    # Enable stain trace (system-level master switch)
    stain-trace-enabled: true

    # Sampling interval: sample 1 out of every 100 records (development environment)
    # Production recommendation: 100000-1000000
    stain-trace-sample-interval: 100

    # Local file storage base directory. This must be configured explicitly.
    stain-trace-file-base-path: /tmp/seatunnel/traces

    # Max events per file (creates new file when reached)
    stain-trace-file-max-events-per-file: 10000

    # Max file size in MB (creates new file when reached)
    stain-trace-file-max-size-mb: 10

    # Flush interval in seconds (batch write interval)
    stain-trace-file-flush-interval-seconds: 10
```

### Step 2: Enable Task-level Switch

Enable in job configuration (`job.conf`):

```hocon
env {
  stain_trace {
    enabled = true
  }

  # Other environment configurations...
  parallelism = 2
  job.mode = "BATCH"
}

source {
  # ... your source configuration
}

transform {
  # ... your transform configuration
}

sink {
  # ... your sink configuration
}
```

### Step 3: Run Your Job

Run your SeaTunnel job, trace data will be saved to the configured local path.

### Step 4: View Trace Files

```bash
# View generated files
ls -lh /tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/

# View file contents (JSON Lines format)
cat /tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/trace-*.jsonl
```

### Step 5: Generate HTML Report Using Analysis Tool

The analyzer is a standalone tool. Place `analyze-traces.sh` and the
`seatunnel-trace-analyzer-*-jar-with-dependencies.jar` in the same directory, then run:

```bash
# Analyze trace files and generate HTML report
./analyze-traces.sh /tmp/seatunnel/traces report.html

# Open report in browser
open report.html  # macOS
xdg-open report.html  # Linux
```

**Building the JAR from source** (development only):
```bash
# From the project root:
mvn clean package -pl seatunnel-trace/seatunnel-trace-analyzer -am
# The JAR is at: seatunnel-trace/seatunnel-trace-analyzer/target/seatunnel-trace-analyzer-*-jar-with-dependencies.jar
```

**Done!** No services needed, trace data is stored in local files.

---

## File Storage Format

### Directory Structure

```
/tmp/seatunnel/traces/            ← stain-trace-file-base-path
└── traces/
    └── {job_id}/
        └── {yyyy-MM-dd}/
            ├── traces-14-30-00-a1b2c3d4.jsonl
            ├── traces-14-30-10-e5f6g7h8.jsonl
            └── ...
```

### File Format

Each file is in JSON Lines format (one JSON object per line).
Each line is an **OTLP `ExportTraceServiceRequest`** — one span per sampled row:

```json
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"seatunnel"}},{"key":"seatunnel.job_id","value":{"stringValue":"123456"}}]},"scopeSpans":[{"scope":{"name":"seatunnel.stain_trace"},"spans":[{"traceId":"0000000000000000000000000000007b","spanId":"000000000000007b","parentSpanId":"","name":"seatunnel.record","kind":1,"startTimeUnixNano":"1708000000000000000","endTimeUnixNano":"1708000001000000000","attributes":[{"key":"seatunnel.table_id","value":{"stringValue":"table1"}},{"key":"seatunnel.sink_task_id","value":{"intValue":"123"}}],"events":[{"name":"SOURCE_EMIT","timeUnixNano":"1708000000000000000","attributes":[{"key":"seatunnel.stage_code","value":{"intValue":"1"}},{"key":"seatunnel.task_id","value":{"intValue":"1"}}]},{"name":"SINK_WRITE_DONE","timeUnixNano":"1708000001000000000","attributes":[{"key":"seatunnel.stage_code","value":{"intValue":"6"}},{"key":"seatunnel.task_id","value":{"intValue":"2"}}]}],"status":{"code":1}}]}]}]}
```

Each line contains:
- `resourceSpans[].resource.attributes`: Job metadata (`service.name`, `seatunnel.job_id`)
- `resourceSpans[].scopeSpans[].scope.name`: `"seatunnel.stain_trace"`
- `spans[]`: One span per sampled row
  - `traceId` / `spanId`: 128-bit / 64-bit hex (zero-extended from internal 64-bit id)
  - `startTimeUnixNano` / `endTimeUnixNano`: First and last stage timestamps in nanoseconds (string)
  - `attributes`: Row metadata (`seatunnel.table_id`, `seatunnel.sink_task_id`)
  - `events[]`: One event per pipeline stage
    - `name`: Stage name (e.g., `SOURCE_EMIT`, `QUEUE_IN`, `SINK_WRITE_DONE`)
    - `timeUnixNano`: Stage timestamp in nanoseconds (string)
    - `attributes`: `seatunnel.stage_code` (int), `seatunnel.task_id` (int)

---

## Configuration Reference

### Engine-level Configuration (seatunnel.yaml)

```yaml
seatunnel:
  engine:
    # ==================== Basic Configuration ====================
    # Enable stain trace (system-level master switch)
    stain-trace-enabled: true

    # Sampling interval: sample 1 out of every N records (default: 100000)
    # Development recommendation: 100-1000
    # Production recommendation: 100000-1000000
    stain-trace-sample-interval: 100000

    # Max traces per worker per second (default: 50)
    # Controls trace volume to prevent event storms
    stain-trace-max-traces-per-second-per-worker: 50

    # Max stage entries per trace (default: 32)
    # 32 covers 99% of pipelines, avoids payload bloat
    stain-trace-max-entries-per-trace: 32

    # ==================== Advanced Configuration ====================
    # Whether to propagate payload to all split outputs (default: false)
    # false: Only first output inherits payload in 1-to-N scenarios
    # true: All splits inherit payload (increases trace count)
    stain-trace-propagate-to-all-splits: false

    # ==================== Local File Storage Configuration ====================
    # File storage base directory. Should use the same storage root as
    # checkpoint.storage.plugin-config.namespace. Example:
    #   checkpoint namespace: /data/seatunnel/checkpoint_snapshot/
    #   trace base path:      /data/seatunnel/traces
    stain-trace-file-base-path: /tmp/seatunnel/traces

    # Max events per file (default: 10000)
    # Creates new file when reached
    stain-trace-file-max-events-per-file: 10000

    # Max file size in MB (default: 10)
    # Creates new file when reached
    stain-trace-file-max-size-mb: 10

    # Flush interval in seconds (default: 10)
    # Batch write interval, balances performance and data integrity
    stain-trace-file-flush-interval-seconds: 10
```

### Task-level Configuration (job.conf)

Control trace enablement via `env` block in job configuration:

```hocon
env {
  # Task-level StainTrace switch
  stain_trace {
    # Enable trace for this task (default: false)
    # Note: Engine-level stain-trace-enabled must also be true
    enabled = true

    # Task-level sampling interval (optional, overrides engine-level)
    # sample_interval = 1000
  }

  # Other environment configurations...
  parallelism = 2
  job.mode = "BATCH"
}
```

### Configuration Reference Table

| Configuration Item | Type | Default | Description |
|-------------------|------|---------|-------------|
| **Engine-level (seatunnel.yaml)** ||||
| `stain-trace-enabled` | Boolean | false | Engine-level master switch, must be true to enable |
| `stain-trace-sample-interval` | Integer | 100000 | Sampling interval: sample 1 out of every N records |
| `stain-trace-max-traces-per-second-per-worker` | Integer | 50 | Max traces per worker per second |
| `stain-trace-max-entries-per-trace` | Integer | 32 | Max stage entries per trace |
| `stain-trace-propagate-to-all-splits` | Boolean | false | Propagate to all split outputs |
| `stain-trace-file-base-path` | String | none | Local file storage base directory. Local file output stays disabled until this path is configured explicitly |
| `stain-trace-file-max-events-per-file` | Integer | 10000 | Max events per file |
| `stain-trace-file-max-size-mb` | Integer | 10 | Max file size (MB) |
| `stain-trace-file-flush-interval-seconds` | Integer | 10 | Flush interval (seconds) |
| **Task-level (job.conf env block)** ||||
| `stain_trace.enabled` | Boolean | false | Task-level switch, requires engine-level also enabled |
| `stain_trace.sample_interval` | Integer | Inherits engine-level | Task-level sampling interval (optional) |

### Activation Conditions

StainTrace final activation condition:

```
effectiveEnabled = engineConfig.stainTraceEnabled && jobEnv.stainTrace.enabled
```

That is: **Both engine-level and task-level must be enabled** for trace to work.

---

## Verification

After running your job, check the following:

### 1. Check If Files Are Generated

```bash
# View file list
ls -lh /tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/

# View file contents
head -n 5 /tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/trace-*.jsonl
```

### 2. Verify Data Integrity

You should see:
- Each line is a complete OTLP JSON object (`resourceSpans` → `scopeSpans` → `spans`)
- Each span's `events[]` contains entries for the **6 basic stages** (SOURCE_EMIT, QUEUE_IN, QUEUE_OUT, TRANSFORM_IN, TRANSFORM_OUT, SINK_WRITE_DONE) — these are the only guaranteed stages; extended stages (101+) require additional instrumentation not yet in place
- Stage timestamps are increasing (S0 < Q+ < Q- < T+ < T- < W!)

### 3. Use Analysis Tool

```bash
./analyze-traces.sh /tmp/seatunnel/traces report.html
open report.html
```

The analysis tool generates an HTML report including:
- End-to-end latency analysis
- Stage duration statistics
- Performance bottleneck identification
- Timeline visualization

### 4. Example Job Characteristics

Default example job `stain_trace_fake_sql_union_to_console.conf`:
- **FakeSource**: Generates 10 records
- **Sql Transform**: Uses LATERAL VIEW EXPLODE, 1 input → 2 outputs
- **Console Sink**: Outputs 20 records
- **Sampling rate**: sample-rate=1 (full sampling)
- **Expected traces**: 10 traces (only first split inherits payload)

---

## Troubleshooting

### Problem 1: Cannot Find Trace Files

**Troubleshooting Steps**:

1. Confirm file storage path:
```bash
# Default path
ls -lh /tmp/seatunnel/traces/traces/

# Check if job_id directory exists
ls -lh /tmp/seatunnel/traces/traces/{job_id}/
```

2. Check permissions:
```bash
# Ensure directory is writable
ls -ld /tmp/seatunnel/traces/
```

3. Check configuration:
```yaml
stain-trace-file-base-path: /tmp/seatunnel/traces  # Confirm path is correct
```

4. View job logs, search for "StainTrace" or "TraceFileWriter"

### Problem 2: No Trace Data

**Troubleshooting Steps**:

1. Confirm engine-level switch is enabled:
```bash
grep -A 5 "stain-trace" config/seatunnel.yaml
```

2. Confirm task-level switch is enabled:
```bash
grep -A 3 "stain_trace" examples/your-job.conf
```

3. Verify configuration:
```hocon
env {
  stain_trace {
    enabled = true  # Must be explicitly enabled
  }
}
```

**Remember**: **Both engine-level and task-level must be enabled** for trace to work!

### Problem 3: Only Partial Stage Data

**Reason**: In Transform's 1-to-N scenario, only the first output inherits payload

**Verify**: Check `stain-trace-propagate-to-all-splits` configuration
```yaml
stain-trace-propagate-to-all-splits: false  # Only first inherits (default)
stain-trace-propagate-to-all-splits: true   # All splits inherit
```

### Problem 4: Cannot See Serialization Events (stages 217-220)

**Symptom**: No RECORD_SERIALIZE_START/END or RECORD_DESERIALIZE_START/END in stage details

**Reason**:
- **Single-node execution**: Data transfers via in-memory queues, no serialization needed, so these stages won't appear
- Only in multi-node cluster execution, when data needs cross-node network transfer, will serialization be triggered

**Solution**:
- To test serialization performance, need to set up a multi-node SeaTunnel cluster
- Single-node testing can ignore serialization events, focus on other stages

### Problem 5: Files Too Large or Too Many

**Adjust Configuration**:

```yaml
# Increase sampling interval to reduce trace count
stain-trace-sample-interval: 1000000  # Sample 1 out of every 1 million

# Increase file size limit
stain-trace-file-max-size-mb: 50

# Increase events per file
stain-trace-file-max-events-per-file: 50000
```

### Problem 6: File Permission Issues

**Error**: `Permission denied` or `Cannot create directory`

**Solution**:
```bash
# Create directory and set permissions
sudo mkdir -p /tmp/seatunnel/traces
sudo chmod 777 /tmp/seatunnel/traces

# Or use user directory
stain-trace-file-base-path: ~/seatunnel/traces
```

---

## Advanced Usage

### Custom Job Configuration

Create your own job configuration file, refer to:
```bash
seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/stain_trace_fake_sql_union_to_console.conf
```

Specify at runtime:
```java
public static void main(String[] args) {
    String configurePath = "/path/to/your/job.conf";
    // ... rest of code
}
```

### Performance Tuning

#### Development Environment Configuration (High Sampling, Easy Debugging)
```yaml
stain-trace-enabled: true
stain-trace-sample-interval: 100  # Sample 1 out of every 100
stain-trace-max-traces-per-second-per-worker: 1000
stain-trace-max-entries-per-trace: 64
stain-trace-file-base-path: /tmp/seatunnel/traces
stain-trace-file-flush-interval-seconds: 5  # More frequent flushing
```

#### Production Environment Configuration (Low Overhead, Large Scale)

> **Path Consistency**: `stain-trace-file-base-path` should use the **same storage root** as
> `checkpoint.storage.plugin-config.namespace`. For example, if your checkpoint namespace is
> `/data/seatunnel/checkpoint_snapshot/`, set the trace path to `/data/seatunnel/traces`.
> For HDFS-backed checkpoints, both should point to the same HDFS prefix so that your storage
> and retention policies apply uniformly.

```yaml
seatunnel:
  engine:
    stain-trace-enabled: true
    stain-trace-sample-interval: 100000  # Sample 1 out of every 100k
    stain-trace-max-traces-per-second-per-worker: 50
    stain-trace-max-entries-per-trace: 32
    # Keep this under the same storage root as checkpoint.storage.plugin-config.namespace
    stain-trace-file-base-path: /data/seatunnel/traces   # matches namespace: /data/seatunnel/checkpoint_snapshot/
    stain-trace-file-max-events-per-file: 50000  # Larger files
    stain-trace-file-max-size-mb: 50
    stain-trace-file-flush-interval-seconds: 30  # Less frequent flushing
    checkpoint:
      storage:
        type: localfile           # or hdfs for HDFS-backed clusters
        plugin-config:
          namespace: /data/seatunnel/checkpoint_snapshot/
```

#### Performance Impact

After optimization, StainTrace performance impact:

| Metric | Value |
|--------|-------|
| CPU overhead at 1/100000 sampling rate | **< 2%** |
| CPU overhead at 1/1000 sampling rate | < 5% |
| Trace payload size per record | ~1KB (32 stages) |
| Arrays.copyOf calls reduction | **-60% ~ -70%** |
| System.currentTimeMillis calls reduction | **-50%** |

### Per-job Sampling Rate Control

Different jobs can use different sampling rates:

```hocon
# High throughput job: low sampling rate
env {
  stain_trace {
    enabled = true
    sample_interval = 1000000  # Sample 1 out of every 1 million
  }
}
```

```hocon
# Debug job: high sampling rate
env {
  stain_trace {
    enabled = true
    sample_interval = 10  # Sample 1 out of every 10
  }
}
```

### Periodic Cleanup of Old Files

```bash
# Delete trace files older than 7 days
find /tmp/seatunnel/traces/traces -type f -name "*.jsonl" -mtime +7 -delete

# Or use crontab for scheduled cleanup
# Clean up files older than 7 days at 2 AM daily
0 2 * * * find /tmp/seatunnel/traces/traces -type f -name "*.jsonl" -mtime +7 -delete
```

---

## Performance Optimization Achievements

StainTrace has been optimized to ensure production readiness:

### Core Optimizations (Completed ✅)

1. **Batch Append API**
   - Append multiple stages at once, reducing array copies
   - Arrays.copyOf calls reduced by **60-70%**

2. **Timestamp Optimization**
   - Batch operations share timestamps
   - System.currentTimeMillis calls reduced by **50%**

3. **Local File Storage**
   - Zero dependencies, no database required
   - JSON Lines / OTLP JSON format, human-readable
   - Standalone analyzer tool generates HTML reports

### Planned Instrumentation (Not Yet Implemented)

4. **Network Serialization Tracing** *(Planned)*
   - RECORD_SERIALIZE_START/END (217-220)
   - Will precisely identify cross-node network transfer bottlenecks
   - Requires hooking into the Hazelcast serialization layer

5. **Flow Control Audit Tracing** *(Planned)*
   - FLOW_CONTROL_AUDIT_START/END (226-227)
   - Will identify backpressure issues

6. **Per-transformer Execution Tracing** *(Planned)*
   - TRANSFORM_EXECUTE_START/END (104-105)
   - Will identify which specific transform in a chain is the bottleneck

---

## Planned Feature Details

### Per-transformer Execution Tracing (TRANSFORM_EXECUTE_START / TRANSFORM_EXECUTE_END)

**Stage codes**: 104 / 105 — **Not yet instrumented**

#### Problem

When a job has a chain of 3 or more Transform plugins, the 6 basic stages can only observe the
**total chain latency** (`TRANSFORM_IN → TRANSFORM_OUT`). If one transform in the chain is slow, it
is impossible from the trace alone to identify which one.

For example, given a chain: `FieldMapper → SqlTransform → CopyField`, the trace today shows:

```
T+ (TRANSFORM_IN)  →  T- (TRANSFORM_OUT)
      ↑                      ↑
  chain starts           chain ends
  (total = 120 ms, but which step took 100 ms?)
```

#### What It Will Look Like Once Instrumented

Each transformer in the chain will emit its own START/END pair:

```
T+ → TRANSFORM_EXECUTE_START[FieldMapper] → TRANSFORM_EXECUTE_END[FieldMapper]
   → TRANSFORM_EXECUTE_START[SqlTransform] → TRANSFORM_EXECUTE_END[SqlTransform]
   → TRANSFORM_EXECUTE_START[CopyField]    → TRANSFORM_EXECUTE_END[CopyField]
   → T-
```

This makes per-transformer bottleneck identification precise:

| Stage | Time (ms) | Duration |
|-------|-----------|----------|
| TRANSFORM_IN | 0 | — |
| TRANSFORM_EXECUTE_START (FieldMapper) | 1 | — |
| TRANSFORM_EXECUTE_END (FieldMapper) | 3 | **2 ms** |
| TRANSFORM_EXECUTE_START (SqlTransform) | 3 | — |
| TRANSFORM_EXECUTE_END (SqlTransform) | 101 | **98 ms** ← bottleneck |
| TRANSFORM_EXECUTE_START (CopyField) | 101 | — |
| TRANSFORM_EXECUTE_END (CopyField) | 103 | **2 ms** |
| TRANSFORM_OUT | 104 | — |

#### Entry Budget

Each transformer consumes **2 payload slots** (START + END). A 5-transformer chain adds 10 entries
on top of the 6 basic stages. The default `stain-trace-max-entries-per-trace: 32` comfortably
accommodates up to 13-transformer chains without truncation.

If you have a very long transform chain and see truncation warnings in logs, increase the limit:
```yaml
stain-trace-max-entries-per-trace: 64
```

#### Current Workaround

Until this feature is instrumented, you can estimate individual transform costs indirectly by:
1. Running the job with only one transform enabled at a time and comparing `T+ → T-` gap
2. Checking transform-specific metrics via SeaTunnel's existing metrics system

---

### Network Serialization Tracing (RECORD_SERIALIZE_START/END · RECORD_DESERIALIZE_START/END)

**Stage codes**: 217 / 218 / 219 / 220 — **Not yet instrumented**

> ⚠️ These stages are **only relevant in multi-node cluster deployments**. In single-node mode,
> data transfers via in-memory queues and serialization never occurs — these stages will not appear
> even after instrumentation.

#### Problem

In a multi-node SeaTunnel cluster, records cross node boundaries via Hazelcast's network queue.
Each crossing incurs a **serialization cost** (on the sender node) and a **deserialization cost**
(on the receiver node). Today this overhead is completely invisible to StainTrace; the gap appears
as dead time between `QUEUE_IN` and `QUEUE_OUT`.

Example (current trace — 2-node cluster, Source on Node A, Sink on Node B):

```
S0 (Node A) → Q+ (Node A) →  ??? 15 ms gap ???  → Q- (Node B) → … → W!
                                   ↑
                          network serialization cost hidden here
```

#### What It Will Look Like Once Instrumented

Each network hop will emit 4 events surrounding the cross-node transfer:

```
Q+ → RECORD_SERIALIZE_START → RECORD_SERIALIZE_END
   → [network transfer] →
     RECORD_DESERIALIZE_START → RECORD_DESERIALIZE_END → Q-
```

This decomposes the previously opaque network gap:

| Stage | Node | Meaning |
|-------|------|---------|
| RECORD_SERIALIZE_START | Sender | Hazelcast begins encoding the row |
| RECORD_SERIALIZE_END | Sender | Encoding complete; bytes handed to socket |
| RECORD_DESERIALIZE_START | Receiver | Hazelcast begins decoding received bytes |
| RECORD_DESERIALIZE_END | Receiver | Row fully reconstructed; ready for processing |

The `task_id` attribute for these events will be a reserved constant (`-1`) identifying the
network serializer rather than a pipeline task.

#### Entry Budget

Each network hop adds **4 entries** (serialize-start/end + deserialize-start/end). With the
default `stain-trace-max-entries-per-trace: 32` and 6 basic stages already consuming 6 slots,
up to **6 cross-node hops** can be traced without truncation.

#### How to Identify If You Are in a Multi-node Cluster

If network serialization stages are of interest, verify your deployment:

```bash
# Check cluster members
curl http://localhost:5801/hazelcast/rest/cluster
# If members > 1, you are in a multi-node cluster
```

#### Current Workaround

Until this feature is instrumented, estimate cross-node serialization overhead by:

```
serialization_overhead ≈ gap(Q-, Q+) - expected_queue_wait_time
```

For a lightly loaded cluster with no backpressure, nearly all of the `Q+ → Q-` gap is
serialization + network latency.

---

### Expected Results

| Scenario | Sampling Rate | Throughput | Expected Overhead |
|----------|---------------|------------|-------------------|
| Production | 1/100000 | 1M records/s | **< 2%** |
| Testing | 1/1000 | 100K records/s | < 5% |
| Development | 1/100 | 10K records/s | < 10% |

---
