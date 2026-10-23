# Zeta Engine Realtime Observability (In-Memory Realtime Data + Async Boundary + Backpressure)

This document describes the design and implementation details of "in-memory realtime data only, configurable async boundaries, and backpressure detection" in Zeta Engine (SeaTunnel Engine).

> Design goals: **Zero intrusion to Transform plugins**, **pure memory**, **realtime window**, **configurable async boundary**, **stable backpressure statistics**.

---

## 1. Goals and Scope

### 1.1 Goals

1. **Async boundary**: Specify certain Transforms as async boundaries through job `env` configuration, preventing them from chaining with upstream, and inserting bounded queues between upstream and the boundary (forming an asynchronous boundary).
2. **Backpressure**: Statistics of blocking time at queue write end (wait time for put/ringBuffer.next), calculate `bpRatio`, and provide queue fill ratio.
3. **Realtime window**: Master side uses in-memory time series (ring/deque) to store recent N minutes of bucket data (default 3 minutes, max 10 minutes), providing REST interface for UI queries (edges/vertices).

### 1.2 Non-goals (Current Implementation Not Covered)

- Long-term historical storage (MySQL/ES/files, etc.) and reports
- Single-record level tracing
- Finer-grained per-subtask metrics UI (current REST aggregates to vertex/queueId)
- Full-link "backpressure root cause detection" automatic diagnosis (current provides visualization and time series metrics)

---

## 2. Configuration Design

### 2.1 Configuration Location and Key

Configuration is under job `env`, using key prefix:

`engine.observability.*`

> Parsing supports dot notation (e.g., `engine.observability.enabled`), corresponding to nested objects in HOCON/YAML.

### 2.2 Configuration Items (Current Implementation)

```hocon
env {
  engine {
    observability {
      # enabled=true: Enable realtime observability (Worker produces metrics, Master aggregates and provides to UI via REST)
      # enabled=false: Completely disabled (Worker doesn't time/inc, so UI/REST cannot display realtime metrics for this job)
      #
      # If enabled is not explicitly configured but async_boundaries or split_sink_io is configured,
      # enabled will be automatically enabled to ensure configuration takes effect.
      enabled = true

      # Master side memory bucket configuration (only affects realtime aggregation/REST)
      bucket_ms = 5000            # Default 5000ms, minimum 1000ms
      retention_minutes = 3       # Default 3min, max 10min, minimum 1min

      # async boundary: List of Transform names (recommended)
      async_boundaries = ["t_enrich", "t_join"]

      # async boundary queue default capacity (0 means use engine default)
      edge_buffer_capacity = 2000

      # Optional: Split Sink IO from upstream execution flow (insert queue)
      # to track "upstream emit blocked by Sink" backpressure
      # Default false (avoid overhead from additional queue). Enable when visualizing Sink backpressure.
      split_sink_io = true

      # Optional: Override capacity by boundary name
      # Structure is List<Map>, each item contains boundary/capacity
      # capacity is validated and clamped to [0, 100000] (exceeds will warn and reduce to upper limit)
      edge_overrides = [
        { boundary = "t_join", capacity = 5000 }
      ]
    }
  }
}
```

### 2.2.1 Transform `name` (Optional, but Recommended)

To make `async_boundaries` easier to configure, Transform supports declaring an optional `name` in configuration, for example:

```hocon
transform {
  Sql {
    name = "t_join"
    plugin_input = "users"
    plugin_output = "users_t1"
    query = "select * from users"
  }
}
env.engine.observability.async_boundaries = ["t_join"]
```

If `name` is not configured, the engine-generated name is used by default (e.g., `Transform[2]-Sql`), in which case `async_boundaries` also needs to use the corresponding default name.

### 2.3 async boundary Semantics (Current Implementation)

The current implementation treats Transform `name` appearing in `async_boundaries` as:

- **"New chain starting point"**: This Transform no longer chains with its upstream (break chain with upstream).
- **"Queue insertion point"**: Insert `IntermediateQueue` (async boundary) between upstream → (async queue) → this Transform's TransformChain.

In other words: `async_boundaries = ["t_join"]` means **"t_join breaks from its upstream, with a bounded queue inserted between"**.

---

## 3. Execution Plan and Chain Breaking Implementation

Overall two steps:

1. **Execution plan generation phase (ExecutionPlan)**: Determine whether Transform can be chained, and generate `TransformChainAction`.
2. **Physical execution flow phase (Flow/Task)**: Insert `IntermediateQueue` for edges satisfying async boundary, and pass capacity configuration to runtime queue.

### 3.1 ExecutionPlanGenerator: Control Transform Chaining

Entry class:

- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/dag/execution/ExecutionPlanGenerator.java`

Core strategy:

- In `collectChainedVertices(...)`, if current `TransformAction.name` matches `async_boundaries` and current chain already has upstream elements, **stop recursive collection**, making this Transform a new chain.

This step ensures that async boundary Transform is not merged into upstream chain, creating a clear boundary for subsequent "queue insertion".

### 3.2 TransformChainAction Metadata: TransformChainConfig

To identify "whether a TransformChain's starting point is a boundary" in physical execution flow phase, added:

- `seatunnel-engine/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/dag/actions/TransformChainConfig.java`

And when creating `TransformChainAction`, write the chain's Transform `name` list to `config`:

- Chain start: `getStartTransformName()`
- Chain end: `getEndTransformName()`

This way `PhysicalPlanGenerator` can determine whether the chain starts with a boundary via `TransformChainAction.getConfig()`.

---

## 4. Physical Execution Flow and Async Queue Insertion

Entry class:

- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/dag/physical/PhysicalPlanGenerator.java`

### 4.1 Insertion Point: splitAsyncBoundaryFromFlow

When building Source's flow list, recursively execute for each flow root:

- `splitAsyncBoundaryFromFlow(root)`: Insert `IntermediateExecutionFlow(IntermediateQueue)` for edges satisfying conditions
- `splitSinkFromFlow(root)`: When `split_sink_io = true`, split Sink IO from upstream execution flow (insert queue)

Async boundary identification conditions (current implementation):

- `flow.getNext()` contains `PhysicalExecutionFlow`
- Its action is `TransformChainAction`
- `TransformChainAction.config` is `TransformChainConfig`
- `TransformChainConfig.startTransformName` matches `async_boundaries`

Structure after insertion:

```
UpstreamFlow
  -> IntermediateExecutionFlow(IntermediateQueue, capacity=...)
      -> TransformChainAction(start = boundaryTransformName)
```

### 4.2 Queue Capacity Delivery

To support "default capacity + override", two-layer passthrough:

1. `IntermediateQueue` adds field `capacity` (0 means use default)
   - `seatunnel-engine/seatunnel-engine-core/.../IntermediateQueue.java`
2. `IntermediateQueueConfig` adds field `capacity`, and writes from `IntermediateQueue` during `setFlowConfig`
   - `seatunnel-engine/seatunnel-engine-server/.../IntermediateQueueConfig.java`
3. When `SeaTunnelTask` creates `IntermediateQueueFlowLifeCycle`, passes `capacity` to TaskGroup, ultimately deciding runtime queue capacity
   - `seatunnel-engine/seatunnel-engine-server/.../SeaTunnelTask.java`

Capacity selection rules:

1. If `edge_overrides[boundaryName]` exists, use override
2. Otherwise use `edge_buffer_capacity`
3. If final is 0, use engine default capacity (BlockingQueue: 2048; Disruptor: 1024)

---

## 5. Runtime Queue and Backpressure Measurement

### 5.1 Runtime Queue Types

Runtime queue is determined by `queueType`:

- BlockingQueue: `TaskGroupWithIntermediateBlockingQueue`
- Disruptor: `TaskGroupWithIntermediateDisruptor`

Both TaskGroups support creating corresponding queue instances by queueId, and statistics for queue write blocking time.

### 5.2 Metric Definitions (Current Implementation)

New metric names:

- `IntermediateQueuePutBlockedNs`: Cumulative queue write blocking time (nanoseconds)
- `IntermediateQueueCapacity`: Queue capacity (expressed through counter's fixed value)

And reuse existing:

- `IntermediateQueueSize`: Queue length (current implementation still uses counter inc/dec to express "current size")

Specific form (subdivided by queueId):

- `IntermediateQueuePutBlockedNs#{queueId}`
- `IntermediateQueueCapacity#{queueId}`
- `IntermediateQueueSize#{queueId}`

Also retain global aggregated size:

- `IntermediateQueueSize` (total size of all queues, inc/dec)

Corresponding definition file:

- `seatunnel-api/src/main/java/org/apache/seatunnel/api/common/metrics/MetricNames.java`

### 5.3 BlockingQueue Backpressure Measurement

Implementation point:

- `seatunnel-engine/seatunnel-engine-server/.../IntermediateBlockingQueue.java`

Statistics method (similar to Flink's "blocking time measurement"):

- First try non-blocking enqueue: `offer()`
- If `offer()` fails (queue full), execute blocking enqueue `put()`, and accumulate `put()` blocking wait time to `IntermediateQueuePutBlockedNs#{queueId}`

Queue length:

- Each enqueue: `IntermediateQueueSize` and `IntermediateQueueSize#{queueId}` both `inc()`
- Each dequeue: corresponding `dec()`

### 5.4 Disruptor Backpressure Measurement

Implementation points:

- Producer side: `RecordEventProducer.onData(...)`
- Consumer side: `RecordEventHandler.onEvent(...)`

Statistics method:

- First try non-blocking sequence request: `ringBuffer.tryNext()`
- If fails (`InsufficientCapacityException`, queue full), call blocking `ringBuffer.next()`, and accumulate wait time to `IntermediateQueuePutBlockedNs#{queueId}`

Queue length:

- On publish: `IntermediateQueueSize` and `IntermediateQueueSize#{queueId}` `inc()`
- On event handler consume and dispatch: `dec()`

Capacity:

- Disruptor's ring buffer requires power of 2, if configured non-power-of-2 will automatically round up to power of 2.

---

## 6. Source Monitoring Scheme (Current Implementation + Notes)

This section supplements Source monitoring design to answer:

- **Is Source blocked by downstream backpressure** (wants to send but can't)
- **Is Source itself unable to keep up** (read/decode/construct record is bottleneck)
- **Does Source have no data** (upstream empty/external system has no data)

> Note: Current code implementation covers "queue dimension backpressure" (Chapter 5), Master memory aggregation (Chapter 8), and adds Source's `read/idle` timing metrics (see 6.3).

### 6.1 Key Idea: Split Source Time into Three Segments

Within a window (bucket), Source thread time is split into:

1. `read_ns`: Actual read/pull/deserialize/construct Record time (busy)
2. `idle_ns`: Poll empty, wait for data, or active sleep/backoff time (idle)
3. `emit_blocked_ns`: Blocking time when writing downstream due to queue/network buffer limits (backpressure)

Derived ratios (within window):

- `busy_ratio = read_ns / bucket_ns`
- `idle_ratio = idle_ns / bucket_ns`
- `backpressure_ratio = emit_blocked_ns / bucket_ns`

> These three ratios can stably distinguish the root cause of "source slow", not just looking at records/s.

### 6.2 Instrumentation Points (No Change to Source Plugins)

Recommended to instrument at engine layer's Source execution loop (e.g., `SourceFlowLifeCycle` / `SourceSeaTunnelTask` main loop), key locations:

1. Time before/after `poll/read`, accumulate to `read_ns` or `idle_ns`
   - If poll returns empty/timeout: record to `idle_ns`
   - If poll returns data and completes decode: record to `read_ns`
2. `sendRecordToNext(...)` (or final downstream output) statistics for write time
   - If downstream is `IntermediateQueue`, may block within `received()`
   - For "direct call to downstream (no queue)" links, cannot infer backpressure from blocking time, need to rely on async boundary/queue for observation

### 6.3 Metric Model (Current Implementation)

Current implementation's Source timing metrics (subdivided by `sourceActionId` using `#{id}` suffix):

- `SourceReadNs#{sourceId}`: Cumulative read time (ns)
- `SourceIdleNs#{sourceId}`: Cumulative idle time (ns)

Notes and suggestions:

- `SourceReadNs` currently uses `pollNext()` call time as measurement; when Source is **directly connected** to downstream without queue (async boundary not configured), `pollNext()` internal `collector.collect()` will synchronously execute downstream logic, causing `SourceReadNs` to include some "write/downstream execution" time.
- To stably distinguish "Source insufficient capacity" from "blocked by downstream", recommend configuring async boundary before key downstream Transforms (insert queue), and combine with Chapter 5 queue's `bpRatio/queueFillRatio` to observe backpressure propagation.

### 6.4 How to Infer: Is Source Unable to Keep Up

Using only "Source sending slow" cannot distinguish cause, need to combine three time segments and queue occupancy:

#### A) Downstream Blocking (backpressure dominant)

- `backpressure_ratio` high
- And `queue_fill_ratio` high (queue near full)

Conclusion: **Source can read, but is blocked by downstream**.

#### B) Source Itself Bottleneck (source bottleneck)

- `backpressure_ratio` low (write not blocking)
- And `queue_fill_ratio` low (queue long-term not full/empty)
- And `busy_ratio` high (most time spent on read/decode)

Conclusion: **Source can't keep up/insufficient capacity**.

#### C) No Data (upstream idle)

- `idle_ratio` high
- Queue not full

Conclusion: **Not backpressure, not source performance issue, but upstream has no data/external system empty**.

### 6.5 Relationship with async boundary

- If Source only has "direct call TransformChain (no queue)" after it, Source's `emit_blocked_ns` is hard to observe in current architecture (because no put blocking will occur).
- Therefore to stably observe Source backpressure, at least need:
  - Pre-Sink queue (engine already has), or
  - Configure `async_boundaries` to insert queue boundary early after Source downstream (recommended for locating backpressure propagation caused by transform).

### 6.6 Transform Monitoring (Current Implementation)

Transform side currently provides basic metrics at **TransformChain** granularity (not single Transform plugin), subdivided by `transformChainActionId` using `#{id}` suffix:

- `TransformProcessNs#{transformChainId}`: Cumulative processing time (ns)
- `TransformRecordsIn#{transformChainId}`: Cumulative input record count
- `TransformRecordsOut#{transformChainId}`: Cumulative output record count

Master aggregation and REST return provides derived metrics:

- `transformBusyRatio = transformProcessNs / (bucketNs * subtaskCount)`
- `transformProcessNsPerRecord = transformProcessNs / transformRecordsIn`

---

## 7. Sink Monitoring Scheme (Current Implementation + Notes)

This section supplements Sink monitoring design to answer:

- **Is Sink causing upstream backpressure** (Sink can't keep up, causing upstream put blocking/queue full)
- **Is Sink itself slow** (external system write slow, flush/commit slow, many retries)
- **Is Sink in "no data to write" state** (upstream insufficient output, queue long-term empty)

> Note: Current code implementation covers "queue dimension backpressure" (Chapter 5), and adds Sink's `write/prepareCommit/commit/abort` timing metrics (see 7.3).

### 7.1 Clarification: What is Sink "Backpressure"

Sink is a terminal operator, usually doesn't have "blocked by downstream" situation, so Sink backpressure should be understood as:

- **Pressure Sink exerts on upstream** (sink pressure), i.e., *"percentage of time upstream is blocked when writing to Sink front buffer/queue"*.

Therefore the most stable measurement for calculating Sink backpressure is: **Use "Sink input edge (queue)" put blocking time**, not looking at whether Sink's records/s decreases.

### 7.2 Key Metrics: Sink Input Edge (Queue) Metrics

As long as pre-Sink queue exists (current engine separates Sink from upstream via `IntermediateQueue` in flow), can calculate for that queue:

- `emit_blocked_ns`: Upstream write blocking time to this queue (cumulative/aggregated by bucket)
- `bp_ratio = emit_blocked_ns / (bucket_ns * upstream_subtask_count)` (consistent with Chapter 8 aggregation measurement)
- `queue_fill_ratio = queue_size / queue_capacity`

When:

- `bp_ratio` high AND `queue_fill_ratio` high

Conclusion: **Sink can't keep up, exerting backpressure on upstream** (upstream "wants to write but can't").

### 7.3 How to Observe Sink Slowness: write/flush/commit Timing (Recommended)

Only having "input queue backpressure" can only indicate Sink is pressuring upstream, but cannot pinpoint whether:

- External system write slow (write blocking)
- checkpoint/commit slow (prepareCommit/commit slow)
- Or internal retry/error caused fluctuation

Current implementation adds metrics (subdivided by `sinkActionId` using `#{id}` suffix):

- `SinkWriteNs#{sinkId}`: Total time for writer.write(...)
- `SinkPrepareCommitNs#{sinkId}`: Total time for writer.prepareCommit(...)
- `SinkCommitNs#{sinkId}`: Total time for committer.commit(...) (if any)
- `SinkAbortNs#{sinkId}`: Total time for committer.abort(...) (if any)
- `SinkRecordsIn#{sinkId}`: Count of writer.write(...) calls

This way can further attribute "cause of backpressure" to external system write/commit paths, and combine with input edge queue's `bpRatio/queueFillRatio` to judge if it's caused by Sink pressure.

### 7.4 How to Determine: Sink Slow vs Upstream Insufficient Output

In the same window, can combine "input queue state + Sink busy time" to judge:

- **Sink Slow (pressuring upstream)**:
  - `queue_fill_ratio` high
  - `bp_ratio` high
  - `sink_write_ns` (or `sink_commit_ns`) ratio high
- **Upstream Insufficient Output (Sink has no data to write)**:
  - `queue_fill_ratio` low (long-term empty)
  - `bp_ratio` low
  - Sink's write related time ratio also low (mostly waiting for data/idle)

### 7.5 Relationship with async boundary

async boundary solves the problem of TransformChain internal unobservability;

Sink side usually already has "pre-Sink queue" as natural observation point, therefore:

- To see "is Sink exerting backpressure" usually doesn't depend on async boundary
- But to see "where backpressure starts in Transform chain" still needs async boundary (Chapter 3/4/5)

---

## 8. Master Memory Realtime Aggregation Design

### 8.1 Why Choose Master Pull + Memory Aggregation

Existing engine metrics pipeline mainly uses "Master RPC pull worker metrics" (`CoordinatorService.getRunningJobMetrics()`), so current implementation follows this pipeline, avoiding introducing extra IMap/Topic/serialization protocol and write amplification.

### 8.2 RealtimeMetricsService

Implementation class:

- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/observability/RealtimeMetricsService.java`

Startup location (Master only):

- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/SeaTunnelServer.java`

Scheduling thread:

- Single-threaded scheduled task, thread name: `realtime-metrics-collector`
- Fixed 1s polling once (`POLL_INTERVAL_MS = 1000`)

Start/stop logic:

- Created and `start()` when Master starts
- `shutdown()` and clean memory store when Server shutdown

### 8.3 Data Model (Memory Bucket)

Maintains a `JobStore` for each jobId:

- `bucketMs`: Bucket duration (from configuration)
- `retentionMinutes`: Retention window (from configuration)
- `Deque<Bucket>`: Bucket list in ascending order by bucketStartMs
- `lastBlockedNsSumByQueueId`: Used to calculate delta for each sample

`Bucket` stores `EdgeBucket` by queueId:

- `emitBlockedNs`: Cumulative blocking time in this bucket (delta accumulation)
- `subtaskCount`: Number of measurement entries for this metric in this sample, used to estimate `bpRatio` denominator
- `queueSizeSum/capacitySum`: Sum of size/capacity across all subtasks at this sample time (current implementation is "last sample value", not average)

### 8.4 Key Calculations

1. Bucket alignment:

`bucketStartMs = floor(nowMs / bucketMs) * bucketMs`

2. `emitBlockedNs` (cumulative within bucket):

- Raw metric is "cumulative counter", need delta:
  `delta = max(0, currentSum - lastSum)`
- Multiple 1s samples within same bucket, accumulate delta to `emitBlockedNs`

3. `bpRatio` (for UI coloring):

`bpRatio = emitBlockedNs / (bucketNs * subtaskCount)`

> Here `subtaskCount` uses the number of measurement entries in current sample, as approximation of "number of participating subtasks".

4. `queueFillRatio`:

`queueFillRatio = queueSizeSum / queueCapacitySum`

---

## 9. REST API (Current Implementation)

Route prefix:

- `/metrics/realtime/*`

Registration location:

- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/JettyService.java`

Implementation Servlet:

- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/rest/servlet/RealtimeMetricsServlet.java`

### 9.1 List Jobs with Observability Enabled

`GET /metrics/realtime/jobs`

Response example (fields may evolve with implementation):

```json
{
  "collector": {
    "pollIntervalMs": 5000,
    "fetchTimeoutMs": 3000,
    "lastCollectStartMs": 1700000000000,
    "lastCollectEndMs": 1700000000123,
    "lastRawMetricsFetchCostMs": 12,
    "lastRawMetricsBlobs": 3,
    "collectFailureCount": 0
  },
  "jobs": [
    {
      "jobId": 12345,
      "bucketMs": 5000,
      "retentionMinutes": 3,
      "latestBucketStartMs": 1700000000000
    }
  ]
}
```

### 9.2 Get Queue (Edges) Time Series for a Job

`GET /metrics/realtime/jobs/{jobId}/edges?windowMs=180000`

Parameter description:

- `windowMs`: Query window (milliseconds), take most recent period from `now` backwards
  - Default: 3 minutes (`180000`)
  - Maximum: 10 minutes (`600000`), exceeding will be truncated to `600000`

Response structure description:

- `edges`: Time series points grouped by `queueId`
- Each point contains:
  - `emitBlockedNs`, `bpRatio`
  - `queueSize`, `queueCapacity`, `queueFillRatio`

> Current implementation's `queueId` is `IntermediateQueue`'s id. To avoid conflict with actionId and distinguish queue types, `PhysicalPlanGenerator` encodes queue id:
>
> - async boundary queue: `queueId = -2 * actionId` (negative even)
> - sink split queue: `queueId = -(2 * actionId + 1)` (negative odd)
>
> REST response additionally provides `targetVertexId` (decoded from `queueId`), for UI to map edge metrics back to DAG vertices (coloring/detail interaction).

---

### 9.3 Get Vertex (source/transform/sink) Time Series for a Job

`GET /metrics/realtime/jobs/{jobId}/vertices?windowMs=180000`

Response structure description:

- `vertices`: Time series points grouped by `vertexId` (aligned with UI DAG's `vertexId`)
- Each point contains (may be 0, depends on vertex type):
  - Source: `sourceReadNs/sourceIdleNs` + `sourceReadRatio/sourceIdleRatio`
  - Transform: `transformProcessNs/transformRecordsIn/Out` + `transformBusyRatio`
  - Sink: `sinkWriteNs/sinkRecordsIn/...` + `sinkBusyRatio/sinkWriteNsPerRecord`

---

## 10. UI Display (Current Implementation)

In Job Detail's DAG view:

- **Edge color**: From gray→green→yellow→orange→red based on `bpRatio` (redder means higher backpressure)
- **Edge thickness**: Based on `queueFillRatio` (thicker means queue closer to full)
- **Click edge**: Open right Drawer, showing edge's `bpRatio/queueFillRatio` and recent time series points
- **Click node**: Open Drawer, showing node basic info and `vertices` recent time series points (Source/Transform/Sink busy/idle ratio)
- **Default window**: UI defaults to request and display most recent 3 minutes of data (max 10 minutes, subject to server window limit)

> Note: Only "edges with IntermediateQueue inserted" have `bpRatio/queueFillRatio` (e.g., pre-Sink queue, configured async boundary queue); pure chaining edges cannot directly calculate backpressure using queue measurement.

---

## 11. Performance and Resource Overhead (Current Implementation Basic Assessment)

### 11.1 Worker Side Overhead

- BlockingQueue: Only extra `nanoTime()` twice when queue full (`offer()` fails)
- Disruptor: Only extra `nanoTime()` twice when queue full (`tryNext()` fails)
- Counters are Counter (inc/dec), overhead consistent with existing `IntermediateQueueSize` magnitude

### 11.2 Master Side Overhead

- Once per second call to `CoordinatorService.getRunningJobMetrics()` (existing capability)
- Memory: Each job max `retentionMinutes * 60 / (bucketMs/1000)` buckets, bucket saves edges/vertices aggregation results (only within window)

---

## 12. Known Limitations and Considerations

1. **queueId → DAG Mapping Limitation**: Current REST outputs queueId dimension time series, and additionally provides `targetVertexId` (decoded from `queueId`), for UI to map edge metrics back to DAG vertices for coloring/interaction.
   - Note: Currently only provides `targetVertexId` (downstream vertex), upstream vertex/precise edge positioning still needs to combine `JobDAGInfo` for more complete mapping.
2. **queueSize/capacity Aggregation Measurement is "Sample Moment Value"**: Same bucket only keeps last sample's size/capacity sum, not average/max.
3. **capacity Expressed Using Counter**: Current implementation uses counter fixed at capacity value, avoiding introducing new Gauge type; UI side needs to understand this metric as "constant".
4. **bpRatio is Estimated**: Denominator uses measurement count as approximation of subtask count, will fluctuate during restart/scale up/down.
5. **Only Master Side Aggregation Provides REST**: Worker nodes don't maintain time series store; non-master node access returns 404/unavailable.

---

## 13. Future Evolution Suggestions (Can Split PRs)

1. **DAG Mapping**: Record queueId and (upVertexId, downVertexId) relationship in physical plan, REST adds `/dag` output to support UI coloring.
2. **TopK Root Cause Candidates**: Output TopK queues and TopK operators based on bpRatio/emitBlockedNs (needs more complete operator metrics).
3. **Source Fine-grained Metrics**: Add `emit_blocked` on top of read/idle (write time/blocking measurement when directly connected without queue), and output `busy/idle/bp` three-segment ratios.
4. **Operator Metrics Extension**: On top of existing Transform/Sink metrics, add error/retry, records/s, and do same bucket aggregation and TopK.
5. **Push Mode (Optional)**: If Master pull pressure is high, can change to Worker push to Hazelcast IMap/Topic, then Master consume and aggregate.

---

## 14. Testing and Verification Suggestions (Covering This Feature)

### 14.1 Unit Tests (UT)

- `seatunnel-engine-server`: Verify plan generation stability + observability config parsing.
- `seatunnel-engine-ui`: Verify percentage formatting and UI rendering.

### 14.2 End-to-End Tests (E2E/IT)

- `JobInfoDagStabilityRestIT`: Verify DAG doesn't "mix" under UI refresh (pipelineEdges signature stable).
- `RealtimeMetricsRestIT`: Verify `/metrics/realtime/*` returns time series data after job submission, and `targetVertexId` mapping correct.
