# StainTrace

## Overview

StainTrace is SeaTunnel's data lineage and end-to-end performance tracing system for tracking complete data flow within the Zeta engine.

## Core Features

- **Framework-level Implementation**: Works with all Connectors automatically, no connector code changes needed
- **6 Basic Stages**: SOURCE_EMIT, QUEUE_IN, QUEUE_OUT, TRANSFORM_IN, TRANSFORM_OUT, SINK_WRITE_DONE (all instrumented; temporal order depends on pipeline topology)
- **Extended Stages**: 40+ fine-grained stage codes defined for future instrumentation (**not yet active — will not appear in trace files**)
- **Local File Storage**: Zero dependencies, OTLP JSON Lines format
- **Performance Optimized**: < 2% overhead with proper sampling configuration

## Tracing Stages

### Basic Stages (1-6)

These 6 stages are the **only currently active tracing stages**. Every sampled record will contain these events.

> **Note**: The temporal order of stages in a trace depends on pipeline topology. When a Transform is fused with the Source task (transform-before-queue): `SOURCE_EMIT → TRANSFORM_IN → TRANSFORM_OUT → QUEUE_IN → QUEUE_OUT → SINK_WRITE_DONE`. When fused with the Sink task: `SOURCE_EMIT → QUEUE_IN → QUEUE_OUT → TRANSFORM_IN → TRANSFORM_OUT → SINK_WRITE_DONE`.

| Stage Code | Name | Description |
|-----------|------|-------------|
| 1 | SOURCE_EMIT (S0) | Source emits data |
| 2 | QUEUE_IN (Q+) | Data enters queue |
| 3 | QUEUE_OUT (Q-) | Data leaves queue |
| 4 | TRANSFORM_IN (T+) | Transform receives data |
| 5 | TRANSFORM_OUT (T-) | Transform outputs data |
| 6 | SINK_WRITE_DONE (W!) | Sink write completed |

### Performance Stages (101-110)

> ⚠️ **Planned — not yet instrumented.** These stage codes are defined in `StainTraceStage` but no production call site exists yet. They will **not appear** in trace files.

- SOURCE_READ_END (101)
- QUEUE_OFFER_START (102)
- QUEUE_DESERIALIZE_END (103)
- TRANSFORM_EXECUTE_START/END (104-105)
- SINK_BATCH_AGGREGATE_END (106)
- SINK_FORMAT_END (107)
- SINK_WRITE_START/END (108-109)
- SINK_COMMIT_END (110)

### Fine-grained Stages (201-220)

> ⚠️ **Planned — not yet instrumented.**

- Source: READ_START (201), SERIALIZE_START/END (202-203)
- Queue: DESERIALIZE_START (204)
- Transform: PARSE_START/END (205-206), BUILD_START/END (207-208)
- Sink: RECEIVE (209), BATCH_AGGREGATE_START/END (210), FORMAT_START/END (211), COMMIT_START/END (212)
- Checkpoint: SNAPSHOT_START/END (213-214), BARRIER_EMIT/RECEIVE (215-216)
- Network (multi-node only): RECORD_SERIALIZE_START/END (217-218), RECORD_DESERIALIZE_START/END (219-220)

### Flow Control Stages (226-227)

> ⚠️ **Planned — not yet instrumented.**

- FLOW_CONTROL_AUDIT_START/END (226-227): Backpressure detection

## Quick Start

### 1. Configure Engine

Edit `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
    stain-trace-enabled: true
    stain-trace-sample-interval: 100000  # Sample 1 out of 100k records
    stain-trace-file-base-path: /data/seatunnel/traces  # Required for local trace files; no default
```

### 2. Enable in Job

Edit your job configuration file:

```hocon
env {
  stain_trace {
    enabled = true
  }
}
```

**Important**: Both engine-level and task-level switches must be enabled.

### 3. Run Job

Execute your SeaTunnel job. Trace files will be generated after
`stain-trace-file-base-path` is configured.

### 4. View Traces

```bash
# Check generated files
ls -lh /tmp/seatunnel/traces/traces/{job_id}/{date}/

# View trace data (JSON Lines format)
cat /tmp/seatunnel/traces/traces/{job_id}/{date}/traces-*.jsonl | jq .
```

### 5. Generate Analysis Report

Place `analyze-traces.sh` and `seatunnel-trace-analyzer-*-jar-with-dependencies.jar` in the
same directory, then run:

```bash
./analyze-traces.sh /tmp/seatunnel/traces report.html
open report.html
```

## Configuration Reference

### Engine Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| stain-trace-enabled | boolean | false | Master switch to enable tracing |
| stain-trace-sample-interval | int | 100000 | Sample 1 out of every N records |
| stain-trace-max-traces-per-second-per-worker | int | 50 | Max traces per worker per second |
| stain-trace-max-entries-per-trace | int | 32 | Max stage entries per trace |
| stain-trace-propagate-to-all-splits | boolean | false | Propagate to all split outputs |
| stain-trace-file-base-path | string | none | File storage base directory. Local file writing stays disabled until this path is configured explicitly |
| stain-trace-file-max-events-per-file | int | 10000 | Max events per file |
| stain-trace-file-max-size-mb | int | 10 | Max file size in MB |
| stain-trace-file-flush-interval-seconds | int | 10 | Flush interval in seconds |

> **Note — Path Consistency with Checkpoint Storage**: It is recommended to configure `stain-trace-file-base-path` under the same storage root as `checkpoint.storage.plugin-config.namespace`. For example, if the checkpoint uses `/data/seatunnel/checkpoint_snapshot/`, set the trace path to `/data/seatunnel/traces`. For production HDFS deployments, both should point to the same HDFS path prefix to ensure storage consistency.
>
> ```yaml
> seatunnel:
>   engine:
>     stain-trace-file-base-path: /data/seatunnel/traces
>     checkpoint:
>       storage:
>         type: hdfs
>         plugin-config:
>           namespace: /data/seatunnel/checkpoint_snapshot/
>           storage.type: hdfs
>           fs.defaultFS: hdfs://namenode:9000
> ```

### Task Configuration

```hocon
env {
  stain_trace {
    enabled = true                # Task-level switch
    sample_interval = 1000        # Optional: override engine-level
  }
}
```

## File Format

### Directory Structure

```
/tmp/seatunnel/traces/
└── traces/
    └── {job_id}/
        └── {yyyy-MM-dd}/
            ├── traces-14-30-00-a1b2c3d4.jsonl
            └── traces-14-30-10-e5f6g7h8.jsonl
```

File name format: `traces-{HH-mm-ss}-{first-8-chars-of-uuid}.jsonl`

### JSON Lines Format

Each line is a complete OTLP `ExportTraceServiceRequest` JSON object (one span per sampled record):

```json
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"seatunnel"}},{"key":"seatunnel.job_id","value":{"stringValue":"123456"}}]},"scopeSpans":[{"scope":{"name":"seatunnel.stain_trace"},"spans":[{"traceId":"000000000000000000000000000000c8","spanId":"00000000000000c8","parentSpanId":"","name":"seatunnel.record","kind":1,"startTimeUnixNano":"1708000000000000000","endTimeUnixNano":"1708000001000000000","attributes":[{"key":"seatunnel.table_id","value":{"stringValue":"table1"}},{"key":"seatunnel.sink_task_id","value":{"intValue":"2"}}],"events":[{"name":"SOURCE_EMIT","timeUnixNano":"1708000000000000000","attributes":[{"key":"seatunnel.stage_code","value":{"intValue":"1"}},{"key":"seatunnel.task_id","value":{"intValue":"1"}}]},{"name":"SINK_WRITE_DONE","timeUnixNano":"1708000001000000000","attributes":[{"key":"seatunnel.stage_code","value":{"intValue":"6"}},{"key":"seatunnel.task_id","value":{"intValue":"2"}}]}],"status":{"code":1}}]}]}]}
```

Field descriptions:
- `resourceSpans[].resource.attributes`: Job metadata (`service.name`, `seatunnel.job_id`)
- `scopeSpans[].scope.name`: Fixed value `"seatunnel.stain_trace"`
- `spans[]`: One span per sampled record
  - `traceId` / `spanId`: 128-bit / 64-bit hex (zero-padded from internal 64-bit id)
  - `startTimeUnixNano` / `endTimeUnixNano`: First and last stage timestamps in nanoseconds (string)
  - `events[]`: One event per pipeline stage
    - `name`: Stage name (e.g. `SOURCE_EMIT`, `QUEUE_IN`, `SINK_WRITE_DONE`)
    - `timeUnixNano`: Stage timestamp in nanoseconds (string)
    - `attributes`: `seatunnel.stage_code` (int), `seatunnel.task_id` (int)

## Performance Impact

| Scenario | Sampling Rate | Throughput | Expected Overhead |
|----------|---------------|------------|-------------------|
| Production | 1/100000 | 1M records/s | < 2% |
| Testing | 1/1000 | 100K records/s | < 5% |
| Development | 1/100 | 10K records/s | < 10% |

## Analysis Tool

The standalone analyzer tool generates HTML reports with:

- End-to-end latency analysis
- Stage duration statistics
- Performance bottleneck identification
- Timeline visualization

## Troubleshooting

### No trace files generated

1. Check both switches are enabled (engine + task level)
2. Verify directory permissions for `stain-trace-file-base-path`
3. Check logs for "StainTrace" or "TraceFileWriter" messages

### Empty trace files

A `TraceFileWriter` is created as soon as the first event for a job arrives, but it is possible for the job to finish before any events are flushed to it, leaving an empty file. These are safe to delete.

### Only partial stage data

This occurs when Transform splits 1 record into N records. By default, only the first output inherits the trace payload. Set `stain-trace-propagate-to-all-splits: true` to trace all splits.

### Cannot see extended stage events (101+, 201+)

Only the **6 basic stages are currently instrumented**. The 101-series, 201-series, network serialization (217-220), and flow control (226-227) stages are defined in the enum but have no production call sites yet — they will not appear in trace files until implemented.

## See Also

- [Quick Start Guide](./stain-trace-quickstart.md)
- [Event Listener](../event-listener.md)
- [Telemetry](telemetry.md)
