---
title: Busyness and Backpressure
---

# Busyness and Backpressure

Zeta Engine collects vertex busyness and queue backpressure metrics for running jobs and displays their recent changes in the Web UI. These metrics help determine whether a performance bottleneck is more likely to be in a Source, TransformChain, Sink, or external system.

The active master retains only a short in-memory time series. This feature is intended for real-time troubleshooting and does not replace a long-term monitoring system such as Prometheus.

## How It Works

Zeta observes a job from two perspectives:

- **Vertex busyness** measures time spent reading from a Source, processing a Transform, or writing to a Sink.
- **Edge backpressure** measures how long a producer waits to write to a bounded queue and how full the queue is.

```mermaid
%%{init: {"theme": "base", "themeVariables": {"background": "#0f1d33", "primaryColor": "#0c2530", "primaryBorderColor": "#2dd4bf", "primaryTextColor": "#f8fbff", "lineColor": "#5db8e2", "secondaryColor": "#1f1a34", "secondaryBorderColor": "#8d7cf6", "secondaryTextColor": "#f8fbff"}}}%%
flowchart LR
    S["Source"] --> T1["TransformChain A"]
    T1 --> Q1["Async boundary queue"]
    Q1 --> T2["TransformChain B"]
    T2 --> QS["Sink input queue"]
    QS --> K["Sink"]

    classDef operator fill:#0c2530,stroke:#2dd4bf,color:#f8fbff,stroke-width:2px
    classDef queue fill:#1f1a34,stroke:#8d7cf6,color:#f8fbff,stroke-width:2px
    class S,T1,T2,K operator
    class Q1,QS queue
    linkStyle default stroke:#5db8e2,stroke-width:2px
```

The Zeta execution plan already isolates a Sink with an intermediate queue. For multiple Transforms combined in one TransformChain, an async boundary adds another queue so that processing capacity can be observed independently on each side of the boundary.

## Enable Real-Time Metrics

### Prerequisites

The Web UI and real-time REST API are served by the Zeta HTTP service. First enable HTTP in `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
```

Real-time metrics are available only for running jobs and can be queried only from the active master.

### Job Configuration

Enable observability in the job's `env` block:

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"

  engine {
    observability {
      enabled = true
      bucket_ms = 5000
      retention_minutes = 3
    }
  }
}
```

| Option | Default | Description |
|---|---|---|
| `enabled` | `false` | Enables vertex and queue metric collection and aggregation. |
| `bucket_ms` | `5000` | Aggregation bucket length in milliseconds. Values below `1000` are raised to `1000`. |
| `retention_minutes` | `3` | In-memory time-series retention. Values are clamped to the range from 1 to 10 minutes. |
| `async_boundaries` | `[]` | Transform names that start new TransformChains. |
| `edge_buffer_capacity` | `0` | Default async-boundary queue capacity. `0` or a negative value uses the engine default. |
| `edge_overrides` | `[]` | Overrides async-boundary queue capacity by boundary Transform name. |

Explicitly setting `enabled = true` is recommended. If `enabled` is absent, a non-empty `async_boundaries` list or `split_sink_io = true` enables metric collection automatically. Explicit `enabled = false` keeps metric collection disabled and prevents configured async boundaries from being inserted.

`split_sink_io` is retained for configuration compatibility. It can trigger the automatic enablement described above, but it does not control Sink queue creation because the current execution plan already isolates each Sink with an intermediate queue. New jobs should enable metrics explicitly with `enabled = true`.

## Configure an Async Boundary

Transform metrics are collected per TransformChain. When multiple Transforms are combined into one chain, only processing metrics for the entire chain are available. Add an async boundary where finer bottleneck location is required:

```hocon
env {
  engine {
    observability {
      enabled = true
      async_boundaries = ["normalize_user", "enrich_user"]
      edge_buffer_capacity = 2048
      edge_overrides = [
        { boundary = "enrich_user", capacity = 8192 }
      ]
    }
  }
}

transform {
  Sql {
    name = "normalize_user"
    plugin_input = "users"
    plugin_output = "normalized_users"
    query = "select * from users"
  }
}
```

`normalize_user` becomes the start of a new TransformChain, and a bounded queue is inserted immediately upstream. Transform names are matched as exact strings, so explicitly configure a stable `name` for each boundary Transform.

Queue capacity is selected in the following order:

1. Use the capacity in `edge_overrides` that matches the boundary Transform name.
2. Otherwise, use `edge_buffer_capacity`.
3. When the capacity is `0`, use the engine default: 2048 for BlockingQueue and 1024 for Disruptor.

Negative `edge_overrides` entries are ignored, and values above 100000 are clamped to 100000. Other malformed entries are ignored with a warning. These capacity options affect only async-boundary queues and do not change the Sink-input queue.

A Disruptor capacity must be a power of two. Zeta rounds any other configured value up to the next power of two.

An async boundary changes the execution topology and adds one bounded queue for each parallel subtask. Use boundaries only where execution isolation or more precise bottleneck location is needed.

## Vertex Metrics

The realtime REST API aggregates vertex metrics by DAG `vertexId`.

| Vertex | Main fields | Description |
|---|---|---|
| Source | `sourceReadRatio`, `sourceIdleRatio` | Time ratios spent reading records and performing empty polls or waits. |
| Transform | `transformBusyRatio`, `transformProcessNsPerRecord`, `transformRecordsIn`, `transformRecordsOut` | Processing time and input/output records for a TransformChain. |
| Sink | `sinkBusyRatio`, `sinkWriteNsPerRecord`, `sinkRecordsIn` | Time spent in `writer.write()` and the number of input records. `prepareCommit`, `commit`, and `abort` durations are returned in separate fields. |

Vertex busyness uses the following calculation and is clamped to the range 0 to 1:

```text
busy_ratio = elapsed_ns / (bucket_ns * subtask_count)
```

Vertex busyness is the proportion of an execution thread's time spent on the corresponding operation within a bucket. It is not CPU utilization.

Source `sourceReadRatio` is based on `pollNext()` duration. If a Source is directly connected to downstream operators, downstream logic executed synchronously in `pollNext()` can also contribute to the read time. Add an async boundary near the Source when Source and downstream processing time must be distinguished.

## Edge Metrics

Backpressure metrics are available only on edges backed by an `IntermediateQueue`, including Sink-input edges and edges created by async boundaries.

| Field | Description |
|---|---|
| `emitBlockedNs` | Cumulative time in the current bucket during which producers wait for queue capacity. |
| `bpRatio` | Producer wait time as a proportion of the bucket duration. |
| `queueSize` | Sum of subtask queue lengths at the latest sample. |
| `queueCapacity` | Sum of subtask queue capacities. |
| `queueFillRatio` | Queue fill ratio at the latest sample. |

`bpRatio` is calculated as follows:

```text
bp_ratio = emit_blocked_ns / (bucket_ns * subtask_count)
```

`bpRatio` shows whether a producer waits because the queue has no available capacity. `queueFillRatio` shows how full the queue is when sampled. A short traffic burst can fill a queue without continuously blocking its producer, so use both metrics and observe multiple buckets.

## Locate a Bottleneck

Start from the edge nearest the Sink and work upstream. Add async boundaries only when a suspicious TransformChain must be narrowed down further.

| Observation | Likely cause | Investigation |
|---|---|---|
| An input edge has persistently high `bpRatio` and `queueFillRatio`, and the downstream vertex also has high busyness | The downstream TransformChain or Sink cannot keep up | Check per-record processing time, external-system latency, and parallelism. |
| Source `sourceIdleRatio` is high and downstream queues remain empty | No upstream data is available, or the Source is waiting for an external system | Check upstream volume, polling intervals, and throttling settings. |
| Source `sourceReadRatio` is high, but the first observable edge is not backpressured | Source reading, deserialization, or a synchronous downstream chain is slow | Add an async boundary close to the Source and observe again. |
| Transform `transformBusyRatio` is high, and its input edge remains full and backpressured | The TransformChain is a bottleneck candidate | Split the chain, inspect expensive expressions, or increase parallelism. |
| Sink `sinkWriteNsPerRecord` is high, and its input edge remains full and backpressured | The external target system is slow | Check batching, flushing, retries, network latency, and target-system load. |

Real-time metrics identify bottleneck candidates; they do not prove root cause on their own. Confirm the diagnosis with throughput, error logs, checkpoint status, and external-system monitoring.

## Web UI

Open Running Jobs in the Web UI and select the Overview of a running job:

- Vertex color represents the latest busyness of the Source, Transform, or Sink.
- Edge color represents the latest `bpRatio`: gray at 0, green between 0 and 5%, yellow from 5% to 20%, orange from 20% to 50%, and red at 50% or higher.
- Edge width increases with `queueFillRatio`.
- Select a vertex or edge to view its recent time series and detailed fields.

The current UI queries the latest 3 minutes and refreshes every 2 seconds. The master collects metrics every 5 seconds, so consecutive UI refreshes can show the same data. The REST API supports query windows of up to 10 minutes.

For complex multi-input DAGs, the UI currently maps a queue to an edge primarily through `targetVertexId`. If several input queues target the same vertex, the UI might not uniquely represent every physical edge. Use the `queueId` values in the REST response to distinguish them.

## REST API

The active master serves real-time endpoints under `/metrics/realtime`:

```bash
# List running jobs and their observability status
curl http://<master-host>:8080/metrics/realtime/jobs

# Query edge metrics for the latest 3 minutes
curl "http://<master-host>:8080/metrics/realtime/jobs/<jobId>/edges?windowMs=180000"

# Query vertex metrics for the latest 3 minutes
curl "http://<master-host>:8080/metrics/realtime/jobs/<jobId>/vertices?windowMs=180000"
```

`windowMs` defaults to 180000 and has a maximum of 600000. See [RESTful API V2](./rest-api-v2.md) for complete request and response schemas.

## Performance Cost and Limitations

- Workers maintain vertex and queue counters and measure producer wait time when a queue is full.
- The active master fetches selected metrics for enabled jobs at a fixed, non-configurable 5-second interval and creates in-memory buckets.
- Each async boundary adds queue memory and cross-thread transfer overhead.
- Only short in-memory windows for running jobs are retained. Persistence, historical reports, and alerting are not provided.
- A new active master does not restore the previous real-time window.
- Transform metrics are reported per TransformChain.
- Queue length and capacity are the last sampled values in a bucket, not averages or peaks.
- Estimated ratios can fluctuate around job recovery, rescaling, or counter resets.

## Related Documentation

- [Runtime Execution Graph](./runtime-execution-graph.md)
- [REST API and Web UI](./rest-api-and-web-ui.md)
- [Web UI](./web-ui.md)
- [RESTful API V2](./rest-api-v2.md)
- [Monitoring Metrics](./telemetry.md)
- [Tuning Guide](./tuning-guide.md)
