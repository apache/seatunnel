# Runtime Execution Graph

## Status

This page is the proposed design contract for [issue #11351](https://github.com/apache/seatunnel/issues/11351). The first delivery should turn the existing Job Detail DAG and realtime observability data into a focused runtime diagnosis graph without adding another metrics pipeline.

The design is intentionally bounded:

- reuse the canonical Zeta job DAG as the graph topology
- reuse active-master in-memory realtime metrics for node and edge health
- keep drill-down data in the existing metric APIs, checkpoint REST endpoints, and UI detail panels
- avoid persistent metric snapshots and long-term replay in the first version

## Problem

SeaTunnel already exposes job topology, vertex metrics, edge metrics, checkpoint history, exceptions, and logs. Operators still need a single path that answers these questions while a job is running:

- which vertex is busy, idle, or waiting right now
- which queue-backed edge is blocked or filling up
- whether the current bottleneck is closer to Source read, Transform processing, Sink write, checkpoint or commit work, or an external system
- where a slowdown appears to start before checking tables and logs

A runtime execution graph should be the diagnosis entry point. It should not become a second monitoring system.

## Existing Foundation

The V1 runtime graph should compose existing Zeta contracts:

| Area | Existing contract |
|---|---|
| Topology | Job Detail already renders the `JobDAGInfo` topology. |
| Vertex metrics | `/metrics/realtime/jobs/{jobId}/vertices` returns recent Source, Transform, and Sink buckets. |
| Edge metrics | `/metrics/realtime/jobs/{jobId}/edges` returns queue-backed edge buckets with `queueId` and `targetVertexId`. |
| Checkpoint state | `/jobs/checkpoints/:jobId` and `/jobs/checkpoints/history/:jobId` expose checkpoint overview and history. |
| Error and logs | Job detail, exception, and log APIs already provide failure context. |

The active master keeps only a short in-memory window. The current collector polls worker metrics every 5 seconds, REST query windows default to 3 minutes, and the maximum window is 10 minutes.

## Goals

1. Show node health directly on the DAG.
2. Show queue-backed edge health directly on the DAG.
3. Make the hottest vertex and most blocked edge visible without switching pages.
4. Let users drill down from a graph object to the existing metric table or detail drawer.
5. Surface checkpoint and task error context near the graph without making them graph-only contracts.
6. Keep large DAG behavior predictable and cheaper than rendering every signal on every element.

## Non-Goals

- distributed tracing
- arbitrary historical replay
- persistent runtime metric snapshots
- connector-specific graph widgets
- a new backend metrics data model parallel to realtime observability
- automatic root cause judgement that replaces operator validation

## Runtime Graph Data Model

### Topology

The graph topology is the current job DAG:

- `jobId`
- `vertexId`
- vertex type, such as Source, Transform, or Sink
- directed edges between vertices
- edge metadata that can be correlated with `targetVertexId`

The runtime graph must not invent a separate topology. If the execution DAG changes in a future version, the graph should follow the engine DAG contract rather than maintaining its own shape.

### Node Runtime State

Each graph node should merge the latest realtime vertex point by `vertexId`.

| Vertex type | Primary visual signal | Supporting fields |
|---|---|---|
| Source | `sourceReadRatio` and `sourceIdleRatio` | `sourceReadNs`, `sourceIdleNs`, `subtaskCount` |
| Transform | `transformBusyRatio` | `transformProcessNsPerRecord`, `transformRecordsIn`, `transformRecordsOut`, `subtaskCount` |
| Sink | `sinkBusyRatio` | `sinkWriteNsPerRecord`, `sinkRecordsIn`, `sinkPrepareCommitNs`, `sinkCommitNs`, `sinkAbortNs`, `subtaskCount` |

The primary node color should represent the metric that best matches the vertex type. For example, Source uses read or idle ratios, Transform uses transform busy ratio, and Sink uses sink busy ratio.

### Edge Runtime State

Only queue-backed edges can expose backpressure metrics in V1. Each graph edge should merge the latest realtime edge point by `targetVertexId` when the REST response provides it, or by decoding `queueId` when necessary.

| Field | Meaning |
|---|---|
| `queueId` | Stable queue metric identifier for realtime aggregation. |
| `targetVertexId` | Downstream vertex used to map a queue metric back to the DAG edge. |
| `bpRatio` | Producer wait time ratio in the bucket. |
| `queueFillRatio` | Latest sampled queue occupancy ratio. |
| `queueSize` | Latest sampled queue size. |
| `queueCapacity` | Latest sampled queue capacity. |

Edge color should represent `bpRatio`. Edge width should represent `queueFillRatio`. The detail drawer should show the raw fields and recent bucket series.

## Stable Contract and Best-Effort Signals

The graph needs to distinguish stable identifiers from diagnostic signals that are useful but sampled.

Stable contract fields:

- `jobId`
- `vertexId`
- `queueId`
- `targetVertexId`
- `bucketMs`
- `fromMs`
- `toMs`
- point timestamp `ts`
- `subtaskCount`

Best-effort diagnostic fields:

- busy ratios
- idle ratios
- per-record time estimates
- queue size
- queue fill ratio
- producer wait ratio
- checkpoint and error summary badges

Best-effort fields may fluctuate around recovery, rescaling, counter resets, or sampling delay. The UI should present them as live diagnosis signals, not as audited accounting values.

## Refresh, Retention, and Cost

V1 should keep the existing refresh and retention model:

- worker counters are collected by the active master
- collection remains a short in-memory window
- default REST query window is 3 minutes
- maximum REST query window is 10 minutes
- UI refresh can be more frequent than the collector interval, but must tolerate repeated buckets
- no runtime graph data is written to disk by default

This keeps runtime graph cost proportional to the existing realtime observability feature instead of adding another polling or persistence loop.

## Checkpoint and Error Context

Checkpoint and error signals should be visible near the graph, but they should remain separate contracts in V1:

- checkpoint overview and history continue to come from checkpoint REST endpoints
- job exceptions and logs continue to come from existing job detail APIs
- the graph may show a small status badge or link, but drill-down should open the existing detail panel

This avoids mixing checkpoint lifecycle data into the realtime metrics endpoint before the lifecycle and retention rules are explicitly agreed.

## Large DAG Degradation

Large DAGs can become unreadable and expensive to repaint. V1 should degrade instead of trying to render every signal at full detail.

Recommended behavior:

- keep topology rendering available
- show a summarized health table with the hottest vertices and most blocked edges
- limit automatic graph refit and animation once the DAG is large
- keep drill-down available for selected vertices and edges
- avoid increasing master collection frequency to compensate for UI complexity

The implementation PR should document the chosen size threshold and keep it as a UI rendering rule, not a backend sampling rule.

## V1 Delivery Plan

1. Keep the active-master realtime REST endpoints as the source of vertex and edge runtime state.
2. Render node color from the latest vertex point for the matching `vertexId`.
3. Render edge color and width from the latest edge point for the matching `targetVertexId`.
4. Show recent point series and raw fields in the existing detail drawer.
5. Add checkpoint and error entry points as nearby context instead of embedding new fields in realtime metrics.
6. Add a large-DAG fallback that lists hottest vertices and most blocked edges when full visual overlay is too noisy.
7. Update REST, Web UI, and operational documentation in English and Chinese.

## Validation Requirements

Implementation should be accepted only when these checks are covered:

- backend realtime edge and vertex response tests cover current fields and `targetVertexId` mapping
- UI tests cover node coloring, edge coloring, edge width, and disabled metrics behavior
- large-DAG fallback is tested with a deterministic synthetic DAG
- docs state that realtime windows are in-memory and best effort
- no new persistent metric table, file, or write path is introduced for V1

## Related Documentation

- [Realtime Observability](./realtime-observability.md)
- [Busyness and Backpressure](./busyness-and-backpressure.md)
- [Web UI](./web-ui.md)
- [RESTful API V2](./rest-api-v2.md)
