---
title: Sink 定时刷新
---

# Sink 定时刷新

Sink 定时刷新允许 Zeta 引擎在没有新输入记录、尚未达到批量阈值或尚未触发下一次
Checkpoint 时，请求 Sink Writer 刷写缓冲数据。该机制主要用于低吞吐或暂时空闲的流式作业，
避免数据长时间停留在内存缓冲区中。

引擎负责调度、信号传递、生命周期管理和失败传播。各 Connector 负责判断当前 Writer 是否可以
安全使用定时刷新，并定义刷写自身缓冲数据的操作。

:::caution 一致性语义

定时刷新独立于 Checkpoint 完成过程，不提供 2PC Exactly-Once 语义。当前 Connector 实现不会在
事务边界与 Checkpoint 对齐的 XA、2PC 或 Exactly-Once Writer 路径中启用该能力。

:::

## 工作原理

在没有引擎支持的情况下，Connector 可以启动后台定时线程，也可以在 `write()` 中检查时间间隔。
后台线程可能与写入、Checkpoint 和关闭流程并发，而 `write()` 中的检查在输入空闲时不会执行。

Zeta 通过正常数据流传递 `FlushSignal`，避免上述限制：

```mermaid
%%{init: {"theme": "base", "themeVariables": {"background": "#0f1d33", "primaryColor": "#0c2530", "primaryBorderColor": "#2dd4bf", "primaryTextColor": "#f8fbff", "lineColor": "#5db8e2", "secondaryColor": "#1f1a34", "secondaryBorderColor": "#8d7cf6", "secondaryTextColor": "#f8fbff"}}}%%
flowchart LR
    timer["Worker 定时线程"] -->|"固定延迟触发"| source["Source 任务"]
    source -->|"FlushSignal"| transform["Transform 链"]
    transform --> queue["中间队列"]
    queue --> sink["Sink 任务"]
    sink -->|"已注册操作"| writer["Connector Writer"]

    queue -. "队列已满或正在关闭" .-> dropped["丢弃信号"]

    classDef operator fill:#0c2530,stroke:#2dd4bf,color:#f8fbff,stroke-width:2px
    classDef queueNode fill:#1f1a34,stroke:#8d7cf6,color:#f8fbff,stroke-width:2px
    class timer,source,transform,sink,writer operator
    class queue,dropped queueNode
    linkStyle default stroke:#5db8e2,stroke-width:2px
```

运行流程如下：

1. 每个 Source 任务在进入运行状态时注册一个固定延迟定时任务。
2. 定时回调创建 `FlushSignal`，并通过与数据记录和 Checkpoint Barrier 相同的同步边界发送到
   Source 下游。
3. Transform 不执行用户 Transform 逻辑，直接转发该信号。
4. 中间队列以非阻塞方式尝试发布该信号。
5. Sink 按数据流顺序消费已成功传递的信号。
6. 如果 Connector Writer 注册了刷新操作，Sink 会在输入处理路径中调用该操作。

定时回调只负责注入信号，不会在定时线程中直接执行 Connector 刷新逻辑。

## 启用定时刷新

### 作业配置

在作业配置的 `env` 块中设置 `sink.flush.interval`：

```hocon
env {
  job.mode = "STREAMING"
  sink.flush.interval = 5000
}
```

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `sink.flush.interval` | `Long` | `0` | 两次定时刷新信号尝试之间的间隔，单位为毫秒。值大于 `0` 时在 Zeta 中启用调度。 |

配置值为 `0` 或负数时，运行时不会注册定时任务。`1`～`99` 毫秒的值可以生效，但会产生警告，
因为过短的间隔会增加队列流量、空刷新次数和 Sink I/O。

该配置仅在 Zeta 中生效。Flink 和 Spark 不会注入 `FlushSignal`。

### Connector 支持

Sink Connector 需要主动支持定时刷新。设置 `sink.flush.interval` 会启动 Source 侧定时任务，
但只有 Writer 支持该功能时，Sink 才会刷写数据。不支持该功能的 Sink 会忽略信号。

请通过所选 Sink Connector 页面顶部的功能清单确认支持状态。如果页面提供定时刷新章节，可在
其中查看 Connector 特有行为和一致性语义。

### Worker 配置

Worker 使用共享的定时执行器注入刷新信号。可以在 `seatunnel.yaml` 中配置其线程数：

```yaml
seatunnel:
  engine:
    timer-flush-pool-size: 1
```

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `timer-flush-pool-size` | `Integer` | `1` | Worker 中用于调度和注入 `FlushSignal` 的线程数，必须大于 `0`。 |

该线程池不执行 Connector 刷新操作。增加线程数不会使 Connector 刷新操作并行执行。

## 顺序与背压

Source 通过同一个同步边界发送数据记录、Checkpoint Barrier 和刷新信号。因此，已成功接收的
信号与 Source 输出保持顺序，并且不会在注入点跨越 Checkpoint Barrier。

刷新信号采用不同于数据和 Barrier 的背压策略：

- `BlockingQueue` 使用非阻塞 `offer()`。
- `Disruptor` 队列使用非阻塞发布。
- 队列已满时丢弃信号，而不是阻塞定时线程。
- 数据记录和 Checkpoint Barrier 继续使用原有的发布路径。

因此，定时刷新是一种尽力而为的低延迟机制。配置间隔控制的是信号尝试频率，不能保证每个
Sink 都严格按照该周期完成刷新。信号被丢弃后，缓冲数据仍可以由后续定时信号、Connector
原有的批量触发条件、Checkpoint 或 Writer 关闭流程处理。

调度使用固定延迟而不是固定频率。第一次信号尝试会等待一个配置间隔；当前回调完成后才开始
计算下一次延迟。长时间暂停后，Zeta 不会通过连续发送多个信号来补偿错过的时间点。

## Checkpoint 与一致性语义

定时刷新和 Checkpoint 处理保持相互独立：

- `FlushSignal` 不会确认或触发 Checkpoint。
- 定时刷新不会替代 Checkpoint 驱动的刷新、快照、Prepare、Commit 或 Abort。
- 定时信号及其完成状态不会写入 Checkpoint 状态。
- 恢复时会通过正常任务生命周期重建 Writer，并重新启动 Source 定时任务。

引擎不会检查 Connector 的事务配置。各 Connector 根据当前实际 Writer 模式决定是否启用该
能力。

| Writer 模式 | 当前行为 |
|---|---|
| At-Least-Once Writer（包括由 Connector 管理的本地事务） | 当刷新操作可以安全重试并继续抛出异常时，可以支持定时刷新。 |
| XA、2PC、Exactly-Once 或事务边界与 Checkpoint 对齐的 Writer | 当前实现不启用定时刷新，因为定时操作可能破坏 Checkpoint 事务边界。 |
| 不支持定时刷新的 Writer | 忽略 `FlushSignal`。 |

定时刷新可以在 Checkpoint 之前使缓冲数据对外可见。如果任务随后失败，恢复流程可能从最近
一次成功的 Checkpoint 重放数据。因此，非事务目标可能收到重复写入。需要去重时，应使用
Connector 提供的幂等能力或确定性键。

## 生命周期与失败处理

- Source 任务进入运行状态时启动定时任务。
- Source 正常关闭时取消定时任务。TaskGroup 取消和完成还提供额外的清理路径，Worker 关闭时
  会停止共享定时线程池。
- 定时任务以 Source 任务为粒度，而不是以整个作业为粒度。信号频率和路由取决于物理执行拓扑。
- Pipeline 准备关闭后，Source、Transform、队列和 Sink 不再接收或转发新的定时刷新信号。
- 信号广播抛出异常时，Zeta 会记录警告，并在下一周期继续尝试。
- 除非 Multi-Table 失败策略隔离了失败表，否则已注册的 Connector 操作抛出的异常会传递到任务
  失败和恢复流程。
- Sink 未注册操作时，收到信号不会执行任何操作。

定时刷新不是作业结束机制。Connector 仍需通过既有的 Checkpoint 和 `close()` 行为处理尚未被
定时任务刷新的缓冲数据。

## Multi-Table 作业

对于 Multi-Table Sink，只要至少一个子 Writer 支持定时刷新，Sink 就会注册一个聚合操作。
调用子 Writer 操作前，Multi-Table Writer 会等待内部数据队列和正在执行的写入完成，避免定时
刷新与异步写线程发生竞争。

定时刷新也遵循已配置的 Multi-Table 失败策略。`FAIL_FAST` 会传递第一个子 Writer 异常；
允许继续处理其他表的策略会隔离失败表，并继续刷新健康表。

## 监控指标

Zeta 在作业指标和 Web UI 中提供以下定时刷新计数和速率：

| 指标 | 含义 |
|---|---|
| `FlushSignalTotal` | 已完成信号广播的 Source 定时回调数量。 |
| `FlushSignalQPS` | Source 生成信号的速率。 |
| `FlushSignalQueueSuccessTotal` | 中间队列成功接收信号的次数。 |
| `FlushSignalQueueFailureTotal` | 中间队列丢弃信号的次数。 |
| `FlushSignalSinkSuccessTotal` | 已注册的 Sink 操作成功完成的次数。 |
| `FlushSignalSinkFailureTotal` | 已注册的 Sink 操作抛出异常的次数。 |
| `FlushSignalSinkQPS` | Sink 操作成功完成的速率。 |

这些指标不要求相等。一个 Source 信号可能经过多个队列或输出，也可能被队列丢弃；不支持定时
刷新的 Sink 不会增加 Sink 操作指标。在 Multi-Table Sink 中，一个聚合操作可能刷新多个子
Writer，但只产生一个 Sink 层结果。

应结合[忙碌度与背压指标](./busyness-and-backpressure.md)分析队列失败计数。队列失败计数持续
增长表示负载下存在被丢弃的定时信号，并不表示数据记录或 Checkpoint Barrier 被丢弃。

## 调优与限制

- 应根据可接受的数据可见延迟和 Connector 刷新开销选择间隔，并避免配置短于一次典型 Sink
  刷新耗时的间隔。
- 避免使用小于 `100` 毫秒的间隔。该配置会产生警告，并增加空刷新、队列流量和外部系统调用。
- 较慢的 Connector 操作会在 Sink 输入处理路径中执行，并可能延迟该 Sink 后续的数据记录和
  Checkpoint Barrier。
- 定时信号采用尽力而为的传递方式，不会持久化或在恢复后重放。
- 如果没有 Sink 支持该功能，启用 `sink.flush.interval` 只会产生信号流量，不会刷新数据。
- 定时刷新目前仅适用于 Zeta。

## 相关文档

- [作业环境配置](../../introduction/configuration/JobEnvConfig.md)
- [Sink 架构](../../architecture/api-design/sink-architecture.md)
- [Checkpoint 机制](../../architecture/fault-tolerance/checkpoint-mechanism.md)
- [Exactly-Once 语义](../../architecture/fault-tolerance/exactly-once.md)
- [忙碌度与背压](./busyness-and-backpressure.md)
- [监控与指标](./telemetry.md)
