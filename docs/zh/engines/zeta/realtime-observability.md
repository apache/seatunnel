# Zeta Engine 实时可观测性（内存实时数据 + Async Boundary + Backpressure）

本文档描述当前在 Zeta Engine（SeaTunnel Engine）中实现的“只保留内存实时数据、支持配置断开点（async boundary）、并能定位背压（backpressure）”的设计与实现细节。

> 设计目标是：**Transform 插件零侵入**、**纯内存**、**实时窗口**、**可配置断开点**、**可稳定统计背压**。

---

## 1. 目标与范围

### 1.1 目标

1. **Async boundary（断开点）**：通过作业 `env` 配置指定某些 Transform 为断开点，使其与上游不再 chaining，并在上游与该断开点之间插入有界队列（从而形成异步边界）。
2. **Backpressure（背压）**：在队列写入端统计阻塞时间（put/ringBuffer.next 的等待时间），计算 `bpRatio`，并同时提供队列占用度（fill ratio）。
3. **实时窗口**：Master 侧使用内存时序（ring/deque）存储最近 N 分钟的 bucket 数据（默认 3 分钟，最大 10 分钟），提供 REST 接口给 UI 查询（edges/vertices）。

### 1.2 非目标（当前实现未覆盖）

- 长期历史存储（MySQL/ES/文件等）与报表
- 单条数据级 tracing
- 更细粒度的 per-subtask 指标 UI（当前 REST 聚合到 vertex/queueId）
- 全链路“背压根因定位”自动诊断（当前提供可视化与时序指标）

---

## 2. 配置设计

### 2.1 配置位置与 key

配置位于作业 `env` 下，使用 key 前缀：

`engine.observability.*`

> 解析方式支持点号路径（例如 `engine.observability.enabled`），对应到 HOCON/YAML 的嵌套对象。

### 2.2 配置项（当前实现）

```hocon
env {
  engine {
    observability {
      # enabled=true：开启 realtime observability（Worker 会产生指标，Master 会聚合并通过 REST 提供给 UI）
      # enabled=false：完全关闭（Worker 不计时/不 inc，因此 UI/REST 也无法展示该作业的 realtime 指标）
      #
      # 若未显式配置 enabled，但配置了 async_boundaries 或 split_sink_io，
      # 则会自动开启 enabled 以保证配置生效。
      enabled = true

      # Master 侧内存 bucket 配置（仅影响 realtime 聚合/REST）
      bucket_ms = 5000            # 默认 5000ms，最小 1000ms
      retention_minutes = 3       # 默认 3min，最大 10min，最小 1min

      # async boundary：Transform 的 name 列表（推荐）
      async_boundaries = ["t_enrich", "t_join"]

      # async boundary 队列默认容量（0 表示使用引擎默认）
      edge_buffer_capacity = 2000

      # 可选：把 Sink IO 从上游执行流拆出来（插入队列），以便统计“上游 emit 被 Sink 顶住”的背压
      # 默认 false（避免额外队列带来的开销）。建议在需要可视化 Sink 背压时开启。
      split_sink_io = true

      # 可选：按 boundary name 覆写容量
      # 结构为 List<Map>，每项包含 boundary/capacity
      # capacity 会被校验并 clamp 到 [0, 100000]（超出会 warn 并降到上限）
      edge_overrides = [
        { boundary = "t_join", capacity = 5000 }
      ]
    }
  }
}
```

### 2.2.1 Transform `name`（可选，但推荐）

为了让 `async_boundaries` 更易于配置，Transform 支持在配置中声明可选的 `name`，例如：

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

如果未配置 `name`，则默认使用引擎生成的名称（形如 `Transform[2]-Sql`），此时 `async_boundaries` 也需要填写对应的默认名称。

### 2.3 async boundary 语义（当前实现）

当前实现把 `async_boundaries` 中出现的 Transform `name` 当作：

- **“新 chain 的起点”**：该 Transform 不再与它的上游 chaining（break 与上游的 chain）。
- **“插入队列的位置”**：在上游 →（async queue）→ 该 Transform 所在的 TransformChain 之间插入 `IntermediateQueue`（异步边界）。

换句话说：`async_boundaries = ["t_join"]` 表示 **“t_join 与其上游之间断开，并在中间插入有界队列”**。

---

## 3. 执行计划与断链实现

整体分两步：

1. **执行计划生成阶段（ExecutionPlan）**：决定 Transform 是否可 chaining，并生成 `TransformChainAction`。
2. **物理执行流阶段（Flow/Task）**：对满足 async boundary 的边插入 `IntermediateQueue`，并把容量配置下发到运行时队列。

### 3.1 ExecutionPlanGenerator：控制 Transform chaining

入口类：

- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/dag/execution/ExecutionPlanGenerator.java`

核心策略：

- 在 `collectChainedVertices(...)` 中，如果当前 `TransformAction.name` 命中 `async_boundaries` 且当前 chain 已经有上游元素，则 **停止递归收集**，从而让该 Transform 成为一个新的 chain。

这一步确保 async boundary 的 Transform 不会被合并进上游 chain，从而为后续“插入队列”创造明确的边界。

### 3.2 TransformChainAction 元数据：TransformChainConfig

为了在物理执行流阶段识别“某个 TransformChain 的起点是否为 boundary”，新增：

- `seatunnel-engine/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/dag/actions/TransformChainConfig.java`

并在创建 `TransformChainAction` 时把链路的 Transform `name` 列表写入 `config`：

- chain 起点：`getStartTransformName()`
- chain 终点：`getEndTransformName()`

这样 `PhysicalPlanGenerator` 可以通过 `TransformChainAction.getConfig()` 判断该 chain 是否以 boundary 开始。

---

## 4. 物理执行流与 async queue 插入

入口类：

- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/dag/physical/PhysicalPlanGenerator.java`

### 4.1 插入点：splitAsyncBoundaryFromFlow

在构建 Source 对应的 flow 列表时，对每个 flow root 递归执行：

- `splitAsyncBoundaryFromFlow(root)`：对满足条件的边插入 `IntermediateExecutionFlow(IntermediateQueue)`
- `splitSinkFromFlow(root)`：当 `split_sink_io = true` 时，把 Sink IO 从上游执行流拆出来（插入队列）

async boundary 的识别条件（当前实现）：

- `flow.getNext()` 中存在 `PhysicalExecutionFlow`
- 其 action 为 `TransformChainAction`
- `TransformChainAction.config` 为 `TransformChainConfig`
- `TransformChainConfig.startTransformName` 命中 `async_boundaries`

插入后的结构示意：

```
UpstreamFlow
  -> IntermediateExecutionFlow(IntermediateQueue, capacity=...)
      -> TransformChainAction(start = boundaryTransformName)
```

### 4.2 队列容量下发

为支持“默认容量 + override”，做了两层透传：

1. `IntermediateQueue` 新增字段 `capacity`（0 表示使用默认）  
   - `seatunnel-engine/seatunnel-engine-core/.../IntermediateQueue.java`
2. `IntermediateQueueConfig` 新增字段 `capacity`，并在 `setFlowConfig` 时从 `IntermediateQueue` 写入  
   - `seatunnel-engine/seatunnel-engine-server/.../IntermediateQueueConfig.java`
3. `SeaTunnelTask` 创建 `IntermediateQueueFlowLifeCycle` 时把 `capacity` 传给 TaskGroup，最终决定运行时队列容量  
   - `seatunnel-engine/seatunnel-engine-server/.../SeaTunnelTask.java`

容量选择规则：

1. 若 `edge_overrides[boundaryName]` 存在，使用 override
2. 否则使用 `edge_buffer_capacity`
3. 若最终为 0，使用引擎默认容量（BlockingQueue：2048；Disruptor：1024）

---

## 5. 运行时队列与背压计量

### 5.1 运行时队列类型

运行时队列由 `queueType` 决定：

- BlockingQueue：`TaskGroupWithIntermediateBlockingQueue`
- Disruptor：`TaskGroupWithIntermediateDisruptor`

两个 TaskGroup 都支持按 queueId 创建对应队列实例，并对队列写入阻塞时间做统计。

### 5.2 指标定义（当前实现）

新增指标名：

- `IntermediateQueuePutBlockedNs`：写入队列阻塞时间累计（纳秒）
- `IntermediateQueueCapacity`：队列容量（通过 counter 的固定值表达）

并沿用已有：

- `IntermediateQueueSize`：队列长度（当前实现仍用 counter 的 inc/dec 表达“当前 size”）

具体落地形式（按 queueId 细分）：

- `IntermediateQueuePutBlockedNs#{queueId}`
- `IntermediateQueueCapacity#{queueId}`
- `IntermediateQueueSize#{queueId}`

同时保留全局聚合 size：

- `IntermediateQueueSize`（所有队列合计的 size，inc/dec）

对应定义文件：

- `seatunnel-api/src/main/java/org/apache/seatunnel/api/common/metrics/MetricNames.java`

### 5.3 BlockingQueue 背压口径

实现点：

- `seatunnel-engine/seatunnel-engine-server/.../IntermediateBlockingQueue.java`

统计方式（近似 Flink 的“阻塞时间口径”）：

- 先尝试非阻塞入队：`offer()`
- 若 `offer()` 失败（队列满），再执行阻塞入队 `put()`，并把 `put()` 的阻塞等待时间累加到 `IntermediateQueuePutBlockedNs#{queueId}`

队列长度：

- 每次入队：`IntermediateQueueSize` 与 `IntermediateQueueSize#{queueId}` 同时 `inc()`
- 每次出队：对应 `dec()`

### 5.4 Disruptor 背压口径

实现点：

- 生产侧：`RecordEventProducer.onData(...)`
- 消费侧：`RecordEventHandler.onEvent(...)`

统计方式：

- 先尝试非阻塞申请序号：`ringBuffer.tryNext()`
- 若失败（`InsufficientCapacityException`，表示队列满），再调用会阻塞的 `ringBuffer.next()`，并把等待时间累加到 `IntermediateQueuePutBlockedNs#{queueId}`

队列长度：

- publish 时 `IntermediateQueueSize` 与 `IntermediateQueueSize#{queueId}` `inc()`
- event handler 消费并下发时 `dec()`

容量：

- Disruptor 的 ring buffer 要求为 2 的幂，若配置非 2 的幂会自动向上取整为 2 的幂。

---

## 6. Source 监控方案（当前实现 + 说明）

本节补充 Source 的监控设计，用于回答：

- **Source 是否被下游背压顶住**（想发但发不出去）
- **Source 是否自身处理不过来**（读/解码/构造记录本身是瓶颈）
- **Source 是否没有数据**（上游空/外部系统无数据）

> 说明：当前代码实现已覆盖 “队列维度背压”（第 5 章）、Master 内存聚合（第 8 章），并新增 Source 的 `read/idle` 计时指标（见 6.3）。

### 6.1 关键思想：把 Source 时间拆三段

在一个窗口（bucket）内，Source 线程时间拆分为：

1. `read_ns`：实际读取/拉取/反序列化/构造 Record 的耗时（忙）
2. `idle_ns`：poll 空、等待数据、或主动 sleep/backoff 的耗时（空闲）
3. `emit_blocked_ns`：向下游写出时，被队列/网络 buffer 限制导致的阻塞耗时（背压）

派生比例（窗口内）：

- `busy_ratio = read_ns / bucket_ns`
- `idle_ratio = idle_ns / bucket_ns`
- `backpressure_ratio = emit_blocked_ns / bucket_ns`

> 这三个比例能稳定区分“source 慢”的根因，而不是仅看 records/s。

### 6.2 插桩点（不改 Source 插件）

推荐在引擎层的 Source 执行循环插桩（例如 `SourceFlowLifeCycle` / `SourceSeaTunnelTask` 的主循环），核心位置：

1. `poll/read` 前后计时，累加到 `read_ns` 或 `idle_ns`
   - 若 poll 返回空/超时：记入 `idle_ns`
   - 若 poll 返回数据并完成 decode：记入 `read_ns`
2. `sendRecordToNext(...)`（或最终写出下游的出口）统计写出耗时
   - 若下游是 `IntermediateQueue`，则 `received()` 内可能阻塞
   - 对于“直接调用下游（无队列）”的链路，无法以阻塞时间推断背压，需要依赖 async boundary/queue 才能观测

### 6.3 指标模型（当前实现）

当前实现的 Source 计时指标（按 `sourceActionId` 使用 `#{id}` 后缀细分）：

- `SourceReadNs#{sourceId}`：累计 read 时间（ns）
- `SourceIdleNs#{sourceId}`：累计 idle 时间（ns）

说明与建议：

- `SourceReadNs` 当前以 `pollNext()` 调用耗时为口径；当 Source 与下游 **无队列直连**（未配置 async boundary）时，`pollNext()` 内部的 `collector.collect()` 会同步执行下游逻辑，导致 `SourceReadNs` 会包含一部分“写出/下游执行”的时间。
- 若要稳定区分 “Source 自身产能不足” 与 “被下游顶住”，建议在 Source 下游关键 Transform 前配置 async boundary（插入队列），并结合第 5 章队列的 `bpRatio/queueFillRatio` 观察背压传导。

### 6.4 如何推断：Source 是否处理不过来

仅用“Source 发得慢”无法区分原因，需要结合三段时间与队列占用：

#### A) 下游顶住（backpressure 主导）

- `backpressure_ratio` 高
- 且 `queue_fill_ratio` 高（队列接近满）

结论：**Source 不是读不动，是被下游阻塞**。

#### B) Source 自身瓶颈（source bottleneck）

- `backpressure_ratio` 低（写出不阻塞）
- 且 `queue_fill_ratio` 低（队列长期不满/偏空）
- 同时 `busy_ratio` 高（大部分时间花在 read/decode）

结论：**Source 处理不过来/产能不足**。

#### C) 没数据（upstream idle）

- `idle_ratio` 高
- 队列不满

结论：**不是背压，也不是 source 性能问题，是上游无数据/外部系统空**。

### 6.5 与 async boundary 的关系

- 如果 Source 后面只有“直接调用的 TransformChain（无队列）”，则 Source 的 `emit_blocked_ns` 在当前架构下难以观测（因为不会发生 put 阻塞）。
- 因此想要稳定观测 Source 背压，至少需要：
  - Sink 前队列（引擎已有），或
  - 配置 `async_boundaries` 在 Source 下游尽早插入队列边界（推荐用于定位 transform 造成的背压传导）。

### 6.6 Transform 监控（当前实现）

Transform 侧当前以 **TransformChain** 为粒度（非单个 Transform 插件）提供基础指标，按 `transformChainActionId` 使用 `#{id}` 后缀细分：

- `TransformProcessNs#{transformChainId}`：累计处理耗时（ns）
- `TransformRecordsIn#{transformChainId}`：累计输入条数
- `TransformRecordsOut#{transformChainId}`：累计输出条数

Master 聚合与 REST 返回会提供派生指标：

- `transformBusyRatio = transformProcessNs / (bucketNs * subtaskCount)`
- `transformProcessNsPerRecord = transformProcessNs / transformRecordsIn`

---

## 7. Sink 监控方案（当前实现 + 说明）

本节补充 Sink 的监控设计，目标是回答：

- **Sink 是否造成上游背压**（Sink 处理不过来，导致上游 put 阻塞/队列满）
- **Sink 是否自身慢**（外部系统写入慢、flush/commit 慢、重试多）
- **Sink 是否处于“没数据可写”**（上游产出不足，队列长期偏空）

> 说明：当前代码实现已覆盖 “队列维度背压”（第 5 章），并新增 Sink 的 `write/prepareCommit/commit/abort` 计时指标（见 7.3）。

### 7.1 先澄清：Sink 的“背压”是什么

Sink 是末端算子，通常不存在“被下游顶住”的情况，所以 Sink 的背压应理解为：

- **Sink 对上游施加的压力**（sink pressure），也就是 *“上游写入 Sink 前缓冲/队列时被阻塞的时间占比”*。

因此计算 Sink 背压的最稳口径是：**使用“Sink 输入边（队列）”的 put 阻塞时间**，而不是看 Sink 的 records/s 是否变小。

### 7.2 关键指标：Sink 输入边（队列）指标

只要存在 Sink 前队列（当前引擎会在 flow 中把 Sink 通过 `IntermediateQueue` 与上游分隔），就可以对该队列计算：

- `emit_blocked_ns`：上游写入该队列的阻塞时间（累计/按 bucket 聚合）
- `bp_ratio = emit_blocked_ns / (bucket_ns * upstream_subtask_count)`（与第 8 章聚合口径一致）
- `queue_fill_ratio = queue_size / queue_capacity`

当：

- `bp_ratio` 高 且 `queue_fill_ratio` 高

结论：**Sink 处理不过来，正在向上游施加背压**（上游“想写但写不进去”）。

### 7.3 Sink 自身慢如何观测：write/flush/commit 计时（建议）

仅有“输入队列背压”只能说明 Sink 压住上游，但无法定位是：

- 外部系统写慢（写入阻塞）
- checkpoint/commit 慢（prepareCommit/commit 慢）
- 或内部重试/错误导致的抖动

当前实现新增的指标（按 `sinkActionId` 使用 `#{id}` 后缀细分）：

- `SinkWriteNs#{sinkId}`：writer.write(...) 总耗时
- `SinkPrepareCommitNs#{sinkId}`：writer.prepareCommit(...) 总耗时
- `SinkCommitNs#{sinkId}`：committer.commit(...) 总耗时（如有）
- `SinkAbortNs#{sinkId}`：committer.abort(...) 总耗时（如有）
- `SinkRecordsIn#{sinkId}`：writer.write(...) 调用条数

这样可以把“造成背压的原因”进一步归因到外部系统写入/commit 等路径，并与输入边队列的 `bpRatio/queueFillRatio` 结合判断是否为 Sink 施压导致。

### 7.4 如何判断：Sink 慢 vs 上游产出不足

在同一窗口内，可结合“输入队列状态 + Sink busy 时间”判断：

- **Sink 慢（对上游施压）**：
  - `queue_fill_ratio` 高
  - `bp_ratio` 高
  - `sink_write_ns`（或 `sink_commit_ns`）占比高
- **上游产出不足（Sink 没数据写）**：
  - `queue_fill_ratio` 低（长期偏空）
  - `bp_ratio` 低
  - Sink 的 write 相关耗时占比也低（多数时间在等数据/空转）

### 7.5 与 async boundary 的关系

async boundary 解决的是 TransformChain 内部不可观测的问题；

Sink 侧通常已经有“Sink 前队列”作为天然观测点，因此：

- 想看“Sink 是否施加背压”通常不依赖 async boundary
- 但想看“背压在 Transform 链中从哪里开始”仍需要 async boundary（第 3/4/5 章）

---

## 8. Master 内存实时聚合设计

### 8.1 为什么选择 Master pull + 内存聚合

现有引擎指标链路以 “Master RPC 拉取 worker metrics” 为主（`CoordinatorService.getRunningJobMetrics()`），因此当前实现沿用该链路，避免引入额外 IMap/Topic/序列化协议与写放大。

### 8.2 RealtimeMetricsService

实现类：

- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/observability/RealtimeMetricsService.java`

启动位置（仅 Master）：

- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/SeaTunnelServer.java`

调度线程：

- 单线程定时任务，线程名：`realtime-metrics-collector`
- 固定 1s 轮询一次（`POLL_INTERVAL_MS = 1000`）

启停逻辑：

- Master 启动时创建并 `start()`
- Server shutdown 时 `shutdown()` 并清理内存 store

### 8.3 数据模型（内存 bucket）

对每个 jobId 维护一个 `JobStore`：

- `bucketMs`：bucket 时长（来自配置）
- `retentionMinutes`：保留窗口（来自配置）
- `Deque<Bucket>`：按 bucketStartMs 递增的 bucket 列表
- `lastBlockedNsSumByQueueId`：用于计算每次采样的 delta

`Bucket` 中按 queueId 存 `EdgeBucket`：

- `emitBlockedNs`：该 bucket 内累计的阻塞时间（delta 累加）
- `subtaskCount`：本次采样中该 metric 的 measurement 条数，用于估算 `bpRatio` 的分母
- `queueSizeSum/capacitySum`：本次采样时全 subtask 的 size/capacity 求和（当前实现为“最后一次采样值”，非平均）

### 8.4 关键计算

1. bucket 对齐：

`bucketStartMs = floor(nowMs / bucketMs) * bucketMs`

2. `emitBlockedNs`（bucket 内累计）：

- 原始指标是“累计 counter”，需要做 delta：
  `delta = max(0, currentSum - lastSum)`
- 同一 bucket 内 1s 采样多次，将 delta 累加到 `emitBlockedNs`

3. `bpRatio`（用于 UI 着色）：

`bpRatio = emitBlockedNs / (bucketNs * subtaskCount)`

> 这里的 `subtaskCount` 取的是当前采样中 measurement 条数，作为“参与统计的 subtask 数”的近似。

4. `queueFillRatio`：

`queueFillRatio = queueSizeSum / queueCapacitySum`

---

## 9. REST API（当前实现）

路由前缀：

- `/metrics/realtime/*`

注册位置：

- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/JettyService.java`

实现 Servlet：

- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/rest/servlet/RealtimeMetricsServlet.java`

### 9.1 列出开启 observability 的 jobs

`GET /metrics/realtime/jobs`

返回示例（字段可能随实现演进）：

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

### 9.2 获取某个 job 的队列（edges）时序

`GET /metrics/realtime/jobs/{jobId}/edges?windowMs=180000`

参数说明：

- `windowMs`：查询窗口（毫秒），从 `now` 往前取最近一段时间的数据
  - 默认：3 分钟（`180000`）
  - 最大：10 分钟（`600000`），超过会被截断为 `600000`

返回结构说明：

- `edges`：按 `queueId` 分组的时序点
- 每个点包含：
  - `emitBlockedNs`、`bpRatio`
  - `queueSize`、`queueCapacity`、`queueFillRatio`

> 当前实现的 `queueId` 是 `IntermediateQueue` 的 id。为了避免与 actionId 冲突并区分队列类型，`PhysicalPlanGenerator` 对队列 id 做了编码：
>
> - async boundary queue：`queueId = -2 * actionId`（负偶数）
> - sink split queue：`queueId = -(2 * actionId + 1)`（负奇数）
>
> REST 响应中额外给出 `targetVertexId`（通过 `queueId` 解码得到），用于 UI 将 edge 指标映射回 DAG 顶点（着色/详情联动）。

---

### 9.3 获取某个 job 的 vertex（source/transform/sink）时序

`GET /metrics/realtime/jobs/{jobId}/vertices?windowMs=180000`

返回结构说明：

- `vertices`：按 `vertexId` 分组的时序点（与 UI DAG 的 `vertexId` 对齐）
- 每个点包含（可能为 0，取决于 vertex 类型）：
  - Source：`sourceReadNs/sourceIdleNs` + `sourceReadRatio/sourceIdleRatio`
  - Transform：`transformProcessNs/transformRecordsIn/Out` + `transformBusyRatio`
  - Sink：`sinkWriteNs/sinkRecordsIn/...` + `sinkBusyRatio/sinkWriteNsPerRecord`

---

## 10. UI 展示（当前实现）

在 Job Detail 的 DAG 视图中：

- **边颜色**：根据 `bpRatio` 从灰→绿→黄→橙→红（越红背压越高）
- **边粗细**：根据 `queueFillRatio`（越粗表示队列越接近满）
- **点击边**：打开右侧 Drawer，展示该边的 `bpRatio/queueFillRatio` 与最近时序点
- **点击节点**：打开 Drawer，展示节点基本信息与 `vertices` 的最近时序点（Source/Transform/Sink 的忙闲比）
- **默认窗口**：UI 默认请求并展示最近 3 分钟的数据（最大 10 分钟，以服务端窗口上限为准）

> 说明：只有“插入了 IntermediateQueue 的边”才有 `bpRatio/queueFillRatio`（例如 Sink 前队列、配置的 async boundary 队列）；纯 chaining 的边无法直接以队列口径计算背压。

---

## 11. 性能与资源开销（当前实现的基本判断）

### 11.1 Worker 侧开销

- BlockingQueue：仅在队列满（`offer()` 失败）时额外 `nanoTime()` 两次
- Disruptor：仅在队列满（`tryNext()` 失败）时额外 `nanoTime()` 两次
- 计数器为 Counter（inc/dec），开销与现有 `IntermediateQueueSize` 一致量级

### 11.2 Master 侧开销

- 每秒一次调用 `CoordinatorService.getRunningJobMetrics()`（已有能力）
- 内存：每个 job 最多 `retentionMinutes * 60 / (bucketMs/1000)` 个 bucket，bucket 保存 edges/vertices 的聚合结果（只保留窗口内）

---

## 12. 已知限制与注意事项

1. **queueId → DAG 映射的限制**：当前 REST 输出的是 queueId 维度时序，并额外给出 `targetVertexId`（由 `queueId` 解码得到），用于 UI 将 edge 指标映射回 DAG 顶点进行着色/联动。
   - 注意：目前只提供 `targetVertexId`（下游顶点），上游顶点/精确边定位仍需要结合 `JobDAGInfo` 做更完整映射。
2. **queueSize/capacity 的聚合口径是“采样时刻值”**：同一 bucket 内只保留最后一次采样的 size/capacity 求和，不是平均值/最大值。
3. **capacity 使用 Counter 表达**：当前实现用 counter 固定为 capacity 值，避免引入新的 Gauge 类型；UI 侧需要按“常量”理解该指标。
4. **bpRatio 是估算**：分母使用 measurement 数作为 subtask 数的近似，重启/扩缩容时会波动。
5. **只有 Master 侧聚合并提供 REST**：Worker 节点上不会维护时序 store；非 master 节点访问会返回 404/不可用。

---

## 13. 后续演进建议（可拆分 PR）

1. **DAG 映射**：在物理计划中记录 queueId 与 (upVertexId, downVertexId) 的关系，REST 增加 `/dag` 输出以支持 UI 着色。
2. **TopK 根因候选**：基于 bpRatio/emitBlockedNs 输出 TopK 队列与 TopK operator（需要更完整的 operator metrics）。
3. **Source 细分指标**：在 read/idle 基础上补齐 `emit_blocked`（无队列直连时的写出耗时/阻塞口径），并输出 `busy/idle/bp` 三段比例。
4. **operator metrics 扩展**：在已具备的 Transform/Sink 指标基础上，补齐 error/retry、records/s 等，并做同样的 bucket 聚合与 TopK。
5. **push 模式（可选）**：若 Master pull 压力大，可改为 Worker push 到 Hazelcast IMap/Topic，再由 Master 消费聚合。

---

## 14. 测试与验证建议（覆盖本次功能点）

### 14.1 单元测试（UT）

- `seatunnel-engine-server`：验证计划生成稳定性 + observability config 解析。
- `seatunnel-engine-ui`：验证百分比格式化与 UI 渲染。

### 14.2 端到端测试（E2E/IT）

- `JobInfoDagStabilityRestIT`：验证 UI 刷新下 DAG 不“混线”（pipelineEdges signature 稳定）。
- `RealtimeMetricsRestIT`：验证 `/metrics/realtime/*` 在提交 job 后能返回时序数据，且 `targetVertexId` 映射正确。
