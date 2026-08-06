---
title: Zeta 基准测试
---

# Zeta 基准测试

本章说明如何在固定资源和固定负载下运行可重复的 Zeta 基准测试，以及如何解释吞吐、延迟和
稳定性而不过度推断结果。只有记录并保持代码版本、JDK、机器、JVM 限制、负载和 JMH 配置
一致，两个结果才具有可比性。

该测试直接运行当前仓库中的 Zeta 代码，适合建立可重复的本地基线。它不包含真实 Connector、
外部系统、网络和多节点开销，因此不能替代生产环境 PoC。

## 工作原理

`seatunnel-benchmarks` 提供两类测试：

- `SeaTunnelRowBenchmark`：测试 Row 创建、读取、复制、投影和大小计算等热点代码。
- `SeaTunnelPipelineBenchmark`：启动单节点嵌入式 Zeta 集群，并通过正常的 Client 和配置
  解析 API 运行完整的有界作业。

MiniCluster 在每个 JMH Trial 的 Setup 阶段启动，不计入测量。作业提交、调度、Source、
Transform、Sink 和作业完成都计入 JMH 测量。

```mermaid
%%{init: {"theme": "base", "themeVariables": {"background": "#0f1d33", "primaryColor": "#0c2530", "primaryBorderColor": "#2dd4bf", "primaryTextColor": "#f8fbff", "actorBkg": "#0c2530", "actorBorder": "#2dd4bf", "actorTextColor": "#f8fbff", "activationBkgColor": "#1f1a34", "activationBorderColor": "#8d7cf6", "noteBkgColor": "#1f1a34", "noteBorderColor": "#8d7cf6", "noteTextColor": "#f8fbff", "signalColor": "#5db8e2", "signalTextColor": "#f8fbff", "labelBoxBkgColor": "#0f1d33", "labelBoxBorderColor": "#5db8e2", "labelTextColor": "#f8fbff", "loopTextColor": "#f8fbff"}}}%%
flowchart LR
    Setup["启动 MiniCluster<br/>不计入 JMH"] -.-> Submit["提交作业<br/>开始计时"]
    Submit --> Source["BenchmarkSource"]
    Source --> Transform["BenchmarkTransform<br/>可选"]
    Transform --> Sink["BenchmarkSink"]
    Sink --> Finish["作业完成<br/>停止计时"]
    Source -. "计划生成时间" .-> Sink
    Sink -.-> Result["Pipeline JSON<br/>吞吐与延迟"]
```

Source 使用基于绝对时间的开环调度。每条记录都携带计划生成时间；当 Zeta 跟不上时，计划
时间仍持续向前推进，因此排队和 backlog 会体现在 event-time latency 中，不会因 Source
等待引擎而被隐藏。

### 测试范围

| JMH 选择器 | 数据链路与目的 |
|---|---|
| `sourceSink` | `Source -> Sink`，作为 Zeta 数据链路基线。 |
| `sourceTransformSink` | `Source -> Transform -> Sink`，增加 Row 复制和确定性的 Transform 工作。 |
| `sourceTransformSinkWithObservability` | 在相同 Transform 链路上开启实时忙碌度观测和有界 async boundary。 |
| `sourceTransformSinkWithTrace` | 在相同 Transform 链路上开启 StainTrace。 |
| `sourceTransformSinkWithObservabilityAndTrace` | 同时开启实时可观测性与 StainTrace，用于隔离组合开销。 |

这些场景保持数据链路一致，只改变 Transform 或可观测能力。Observability 场景测量指标采集
与 async boundary 的开销，不会人为限制 Sink 或制造背压。要测试过载，应将
`offeredRatePerSecond` 设置到高于引擎容量，再检查吞吐、P99 和延迟增长。

### 默认测试资源

| 配置 | 默认值 |
|---|---:|
| JVM 堆内存 | 固定 4 GiB `-Xms` / `-Xmx` |
| JVM 可见处理器 | 4 |
| 垃圾回收器 | G1，并启用 pre-touch |
| Zeta slot / Pipeline 并行度 | 12 / 4 |
| 每次 invocation 记录数 | 1,000,000 |
| 输入速率 | 600,000 行/秒 |
| Payload 大小 | 256 个字符 |
| Transform 工作量 | 每行 64 次 hash 操作 |
| StainTrace 采样间隔 | 10,000 行 |
| StainTrace 文件刷新间隔 | 1 秒 |
| JMH fork | 3 |
| 预热 / 测量 iteration | 3 / 5 |

这些 JVM 限制由 Benchmark 类传给 fork JVM，启动时不需要额外配置堆内存。在默认负载和
并行度下，StainTrace 每次 invocation 约采样 100 行，每个 Worker 每秒约采样 15 行，低于
默认的每 Worker 每秒 50 条限制。1 秒刷新间隔保证本地 Trace 输出发生在每个测量作业内，
避免文件写入延后到后续多个 invocation。

## 运行基准测试

### 构建 Benchmark Runner

```bash
./mvnw -Pbenchmark -pl seatunnel-benchmarks -am -DskipTests package
```

查看全部 JMH 方法：

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar -l
```

### 运行完整 Pipeline

评估固定负载时，建议先固定一条链路和一种 Payload，并保存标准 JMH JSON。下面的命令用于
检查 Zeta 能否持续处理每秒计划输入的 600,000 行数据：

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar \
  'sourceTransformSink$' \
  -p offeredRatePerSecond=600000 \
  -p parallelism=4 \
  -p payloadSize=256 \
  -p transformOperations=64 \
  -rf json \
  -rff seatunnel-benchmarks/target/zeta-pipeline-result.json
```

寻找容量边界时，每次只修改 `offeredRatePerSecond`。先从高于预期容量的速率开始，再逐步
降低，直到输出完整并且 P99 不再随运行持续增长。例如，可以先设置
`-p offeredRatePerSecond=1000000`，从高于默认负载的位置开始扫描容量。只有在
测量不控速的吞吐上限时才使用 `0`；该模式没有开环调度，无法暴露输入排队产生的延迟。

使用默认负载运行全部五个 Pipeline 场景：

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar SeaTunnelPipelineBenchmark
```

JMH 支持按类名、方法名或正则选择测试。例如运行全部 Trace 相关方法：

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar \
  'SeaTunnelPipelineBenchmark.*Trace'
```

JMH 的选择器本质上是正则表达式。只运行一个方法时应在末尾加 `$`；否则
`sourceTransformSink` 还会匹配所有以该文本开头的方法。

### 运行 SeaTunnelRow 微基准

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar SeaTunnelRowBenchmark \
  -rf json \
  -rff seatunnel-benchmarks/target/seatunnel-row-result.json
```

快速功能验证时可以增加 `-f 1 -wi 0 -i 1 -r 1s` 缩短运行时间。没有预热且只有一个样本的
结果不能用于性能结论。

## 指标

### 样本有效性

解释性能指标前，先检查：

- `processed_rows` 等于 `expected_rows`；
- `sourceSink` 的 `checksum` 为 0；
- 所有 Transform 场景的 `checksum` 非 0。

这些条件用于拒绝输出不完整的运行，并证明 Transform 工作确实到达 Sink。

### JMH 指标

| 字段 | 说明 |
|---|---|
| `Score` | Pipeline Benchmark 每秒处理的行数，越大越好；Row 微基准仍使用 `ops/ms`。 |
| `Error` | 根据本次 JMH 运行内部样本计算的不确定性。 |
| `Cnt` | 参与聚合的 Measurement 样本数，不是处理行数。 |
| `Units` | Score 的单位。 |

`SeaTunnelPipelineBenchmark` 声明每个 invocation 包含 1,000,000 个逻辑操作，因此 JMH
会把一次完整 Job 换算成处理行数，并使用 `ops/s` 输出。JMH 计时包含作业提交、调度和
完整链路执行；它和只计算 Sink 接收区间的 `throughput_rows_per_second` 不是同一个测量边界。

JMH `Error` 不包含不同机器之间的差异，不能只根据两台机器上的 JMH 置信区间是否重叠
来判断性能回归。

### Pipeline 指标

每次 invocation 会在 `seatunnel-benchmarks/target/pipeline-results` 写入一份 JSON：

| 字段 | 说明 |
|---|---|
| `offered_rate_rows_per_second` | Source 计划输入的目标速率；它表示负载，不是实际吞吐。 |
| `throughput_rows_per_second` | Sink 从接收第一条到最后一条记录期间的实际完成速率。 |
| `event_time_latency_p50_ms` | 从计划生成到 Sink 接收的中位耗时。 |
| `event_time_latency_p95_ms` / `event_time_latency_p99_ms` | 尾延迟；引擎跟不上时包含 backlog 等待。 |
| `event_time_latency_max_ms` | 最大记录延迟，应和百分位一起分析。 |
| `first_half_p99_ms` / `second_half_p99_ms` | 前半段和后半段 P99，用于判断 backlog 是否持续增长。 |
| `latency_growth_ratio` | `(后半段 P99 + 1) / (前半段 P99 + 1)`；大于 1 表示延迟在恶化。 |
| `latency_overflow_rows` | 延迟超过 Histogram 统计范围的记录数。 |
| `sustainable` | 默认要求输出完整、P99 不超过 1,000 ms、增长比例不超过 1.20。 |

`sustainable` 是便捷保护条件，不是通用 SLA。最终是否满足要求，应由目标业务的吞吐和延迟
目标决定。

## 判断测试结果

建议先判断负载是否稳定，再定位是哪一部分造成差异。

| 观察结果 | 结论 | 下一步 |
|---|---|---|
| 输出完整，实际吞吐接近输入速率，前后半段 P99 相近 | 当前负载处于稳态 | 提高输入速率，继续寻找容量边界。 |
| 实际吞吐低于输入速率，后半段 P99 持续升高 | 正在积累 backlog，超过当前配置的可持续容量 | 降低输入速率，或增加资源与并行度后重测。 |
| `sourceSink` 稳定，`sourceTransformSink` 明显变慢 | Transform 工作是主要增量 | 调整 `transformOperations`，检查 Row copy 和 Transform 热点。 |
| 基础 Transform 稳定，Observability 或 Trace 场景明显变慢 | 对应功能产生可见开销 | 使用相同参数重复运行，比较单独开启与同时开启的结果。 |
| 同一 Commit 的全部 Benchmark 在某次运行中同时大幅变化 | 执行机器的 CPU 性能可能不同 | 将本次标记为不确定，检查 CPU 指纹，不更新精细性能基线。 |

容量评估应使用多个固定速率进行扫描，每个速率都在同一台空闲机器上通过独立 JVM 重复运行，
并保留全部样本。比较两个场景或两个 Commit 时，JDK、机器、Payload、并行度、输入速率和
Transform 工作量必须相同。

## 可视化

使用 `-rf json -rff <file>` 生成 JMH JSON，打开
[JMH Visualizer](https://jmh.morethan.io/)，按方法名和参数比较 Score、Error、fork 和
iteration。

JMH Visualizer 会把参数值拼成标签。例如 `600000:4:256:64` 依次表示
`offeredRatePerSecond=600000`、`parallelism=4`、`payloadSize=256` 和
`transformOperations=64`，顺序与图例一致。JMH Score 包含作业提交、调度和完整 Pipeline
执行时间。判断该负载是否可持续时，还需要结合 Pipeline JSON 的吞吐、延迟和完整性字段。

`pipeline-results` 下的 JSON 不是 JMH 格式，应直接查看，或者使用
`tools/benchmarks/save_jmh_result.py` 和 `tools/benchmarks/regression_report.py` 生成
标准化 JSON 和 Markdown 报告。

## 开销与限制

- Pipeline Benchmark 会在本机启动嵌入式 Zeta 集群，需要至少 4 GiB 可用堆内存。
- 完整测试包含 3 个 fork、3 次预热和 5 次测量；运行全部五个场景会花费较长时间。
- `ActiveProcessorCount=4` 只限制 JVM 可见处理器数量，不提供操作系统级 CPU 绑核。
- 精细性能对比应使用固定机器，或在同一台其他负载保持空闲的机器上交替运行 Base 与
  Candidate。
- 本测试不包含真实 Connector、外部消息队列、网络、磁盘和多节点通信成本。

测量生产端到端性能时，应使用所需 Connector 和外部系统重复实验，并结合错误日志、Checkpoint
状态和外部系统监控解释结果。

## 参考资料

1. Andy Georges、Dries Buytaert、Lieven Eeckhout，
   [Statistically Rigorous Java Performance Evaluation](https://dri.es/files/oopsla07-georges.pdf)，
   OOPSLA 2007。
2. Tomas Kalibera、Richard Jones，
   [Rigorous Benchmarking in Reasonable Time](https://dl.acm.org/doi/10.1145/2491894.2464160)，
   ISMM 2013。
3. Jeyhun Karimov 等，
   [Benchmarking Distributed Stream Data Processing Systems](https://arxiv.org/pdf/1802.08496)，
   ICDE 2018。

## 相关文档

- [忙碌度与背压](./busyness-and-backpressure.md)
- [监控与指标](./telemetry.md)
- [调优指南](./tuning-guide.md)
