---
title: 忙碌度与背压
---

# 忙碌度与背压

Zeta Engine 可以采集运行中作业的节点忙碌度和队列背压指标，并在 Web UI 中展示最近一段时间的变化。通过这些指标，可以判断性能瓶颈更可能位于 Source、TransformChain、Sink，还是外部系统。

该功能只在 Active Master 的内存中保留短期时序数据，适用于实时排障，不替代 Prometheus 等长期监控系统。

## 工作原理

Zeta 从两个维度观察作业运行状态：

- **节点忙碌度**：统计 Source 读取、Transform 处理和 Sink 写入所消耗的时间。
- **边背压**：统计生产者向有界队列写入时的等待时间，并记录队列填充率。

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

Zeta 的执行计划已经使用中间队列隔离 Sink。对于被合并到同一 TransformChain 的多个 Transform，可以通过 async boundary 增加新的队列边界，以便分别观察边界两侧的处理能力。

## 启用实时指标

### 前置条件

Web UI 和实时 REST API 由 Zeta HTTP 服务提供。首先在 `seatunnel.yaml` 中开启 HTTP 服务：

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
```

实时指标只适用于运行中的作业，并且只能从 Active Master 查询。

### 作业配置

在作业配置的 `env` 中启用可观测性：

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

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `enabled` | `false` | 是否采集并聚合节点和队列指标。 |
| `bucket_ms` | `5000` | 聚合时间桶长度，单位为毫秒；小于 `1000` 时会调整为 `1000`。 |
| `retention_minutes` | `3` | 内存时序保留时间；超出 1～10 分钟的范围时会被限制在该范围内。 |
| `async_boundaries` | `[]` | 作为新 TransformChain 起点的 Transform 名称列表。 |
| `edge_buffer_capacity` | `0` | async boundary 队列的默认容量；`0` 或负数表示使用引擎默认值。 |
| `edge_overrides` | `[]` | 按边界 Transform 名称覆盖 async boundary 队列容量。 |

建议显式配置 `enabled = true`。未配置 `enabled` 时，非空的 `async_boundaries` 或 `split_sink_io = true` 会自动开启指标采集。显式配置 `enabled = false` 时，指标采集保持关闭，并且不会插入已配置的 async boundary。

`split_sink_io` 仅用于配置兼容，可以触发上述自动开启逻辑，但不控制 Sink 队列的创建，因为当前执行计划已经通过中间队列隔离每个 Sink。新作业应使用 `enabled = true` 显式开启指标。

## 配置 Async Boundary

Transform 指标以 TransformChain 为粒度。如果多个 Transform 被合并到同一 chain，只能观察整条 chain 的处理时间。可以在需要进一步定位的位置插入 async boundary：

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

`normalize_user` 会成为新 TransformChain 的起点，其上游会插入一个有界队列。Transform 名称按字符串精确匹配，因此建议为边界 Transform 显式配置稳定的 `name`。

队列容量遵循以下优先级：

1. 使用 `edge_overrides` 中与边界 Transform 名称匹配的容量。
2. 没有匹配项时使用 `edge_buffer_capacity`。
3. 容量为 `0` 时使用引擎默认值：BlockingQueue 为 2048，Disruptor 为 1024。

`edge_overrides` 中的负数配置项会被忽略，超过 100000 的值会被限制为 100000；其他格式错误的配置项会记录警告并被忽略。这些容量配置只影响 async boundary 队列，不会修改 Sink 输入队列。

Disruptor 的容量必须是 2 的幂。配置其他值时，Zeta 会向上取整到下一个 2 的幂。

Async boundary 会改变执行拓扑，并为每个并行 subtask 增加一个有界队列。应只在需要隔离执行或定位瓶颈的位置使用。

## 节点指标

实时 REST API 按 DAG `vertexId` 聚合节点指标。

| 节点 | 主要字段 | 说明 |
|---|---|---|
| Source | `sourceReadRatio`、`sourceIdleRatio` | 分别表示读取数据和空轮询或等待所占的时间比例。 |
| Transform | `transformBusyRatio`、`transformProcessNsPerRecord`、`transformRecordsIn`、`transformRecordsOut` | 以 TransformChain 为粒度统计处理时间和输入输出记录数。 |
| Sink | `sinkBusyRatio`、`sinkWriteNsPerRecord`、`sinkRecordsIn` | 统计 `writer.write()` 的耗时和输入记录数。`prepareCommit`、`commit` 和 `abort` 耗时通过独立字段返回。 |

节点忙碌度使用以下计算方式，并限制在 0～1：

```text
busy_ratio = elapsed_ns / (bucket_ns * subtask_count)
```

节点忙碌度表示执行线程在一个时间桶内用于对应操作的时间比例，不等同于 CPU 使用率。

Source 的 `sourceReadRatio` 以 `pollNext()` 调用耗时为基础。如果 Source 与下游直接连接，`pollNext()` 中同步执行的下游逻辑也可能计入读取时间。需要区分 Source 和下游耗时时，可以在靠近 Source 的 Transform 前增加 async boundary。

## 边指标

只有存在 `IntermediateQueue` 的边才会产生背压指标，包括 Sink 输入边和 async boundary 创建的边。

| 字段 | 说明 |
|---|---|
| `emitBlockedNs` | 当前时间桶内，生产者等待队列可写的累计时间。 |
| `bpRatio` | 生产者等待时间占时间桶的比例。 |
| `queueSize` | 最近一次采样时，各 subtask 队列长度之和。 |
| `queueCapacity` | 各 subtask 队列容量之和。 |
| `queueFillRatio` | 最近一次采样时的队列填充率。 |

`bpRatio` 的计算方式为：

```text
bp_ratio = emit_blocked_ns / (bucket_ns * subtask_count)
```

`bpRatio` 反映生产者是否因为队列没有可用空间而等待；`queueFillRatio` 反映采样时队列有多满。队列可能因短时流量突发而变满，但尚未持续阻塞生产者，因此应结合两个指标并观察多个时间桶。

## 判断瓶颈位置

建议从最靠近 Sink 的边开始向上游排查，再通过 async boundary 缩小范围。

| 观察结果 | 可能原因 | 排查方向 |
|---|---|---|
| 输入边的 `bpRatio` 和 `queueFillRatio` 持续较高，下游节点忙碌度也较高 | 下游 TransformChain 或 Sink 处理能力不足 | 检查单条处理耗时、外部系统延迟和并行度。 |
| Source 的 `sourceIdleRatio` 较高，后续队列长期为空 | 上游暂无数据，或 Source 正在等待外部系统 | 检查上游数据量、轮询间隔和限流配置。 |
| Source 的 `sourceReadRatio` 较高，但第一条可观测边没有背压 | Source 读取、反序列化或同步下游链路较慢 | 在靠近 Source 的 Transform 前增加 async boundary 后再次观察。 |
| Transform 的 `transformBusyRatio` 较高，其输入边持续满并产生背压 | 该 TransformChain 是瓶颈候选 | 拆分 chain、检查复杂表达式或提高并行度。 |
| Sink 的 `sinkWriteNsPerRecord` 较高，其输入边持续满并产生背压 | 外部目标系统写入较慢 | 检查批量、flush、重试、网络和目标系统负载。 |

实时指标用于识别瓶颈候选，不能单独证明根因。最终判断还应结合吞吐量、错误日志、Checkpoint 状态和外部系统监控。

## Web UI

打开 Web UI 的 Running Jobs，进入运行中作业的 Overview 页面：

- 节点颜色表示 Source、Transform 或 Sink 的最新忙碌度。
- 边颜色表示最新 `bpRatio`：0 为灰色，0～5% 为绿色，5%～20% 为黄色，20%～50% 为橙色，50% 及以上为红色。
- 边的粗细随 `queueFillRatio` 增加。
- 点击节点或边可以查看最近时序和详细字段。

当前 UI 查询最近 3 分钟的数据，并每 2 秒刷新一次。Master 每 5 秒采集一次指标，因此连续两次刷新显示相同数据属于正常现象。REST API 支持查询最长 10 分钟的窗口。

复杂多输入 DAG 目前主要通过 `targetVertexId` 将队列映射到 UI 边。同一目标节点存在多条输入队列时，UI 可能无法唯一展示每条物理边，可以通过 REST 响应中的 `queueId` 进一步区分。

## REST API

实时接口由 Active Master 提供，路由前缀为 `/metrics/realtime`：

```bash
# 列出运行中作业及其 observability 状态
curl http://<master-host>:8080/metrics/realtime/jobs

# 查询最近 3 分钟的边指标
curl "http://<master-host>:8080/metrics/realtime/jobs/<jobId>/edges?windowMs=180000"

# 查询最近 3 分钟的节点指标
curl "http://<master-host>:8080/metrics/realtime/jobs/<jobId>/vertices?windowMs=180000"
```

`windowMs` 默认为 180000，最大值为 600000。完整的请求和响应结构见 [RESTful API V2](./rest-api-v2.md)。

## 性能开销与限制

- Worker 会为节点和队列维护计数器，并在队列满时统计生产者等待时间。
- Active Master 当前以固定、不可配置的 5 秒间隔拉取已开启作业的目标指标，并在内存中生成时间桶。
- 每个 async boundary 会增加队列内存和线程间传递开销。
- 只保留运行中作业的短期内存数据，不支持持久化、历史报表和告警。
- Master 切换后不会恢复之前的实时窗口。
- Transform 指标以 TransformChain 为粒度。
- 队列长度和容量是时间桶内最后一次采样值，不是平均值或峰值。
- 作业恢复、扩缩容或 counter 重置时，估算比例可能出现短时波动。

## 相关文档

- [运行时执行图](./runtime-execution-graph.md)
- [REST API 与 Web UI](./rest-api-and-web-ui.md)
- [Web UI](./web-ui.md)
- [RESTful API V2](./rest-api-v2.md)
- [监控与指标](./telemetry.md)
- [调优指南](./tuning-guide.md)
