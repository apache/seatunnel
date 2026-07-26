---
sidebar_position: 16
---

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# 引擎受管 Source 运行时

引擎受管 Source 运行时是 [STIP-31](https://github.com/apache/seatunnel/issues/11558)
引入的实验性 Zeta 执行通道。它把 Reader 和 Split Enumerator 的 checkpoint 可见状态统一放入
引擎拥有的事件循环中串行执行，使连接器不再需要自行用 checkpoint lock 协调引擎回调。

该功能默认关闭。只有以下条件同时满足，现有作业才会在新部署时进入受管通道：

1. `managed-source-runtime.enabled` 为 `true`。
2. 连接器名称在 `connector-allowlist` 中。
3. 连接器声明了兼容的 `ManagedSourceCapability`。
4. 运行时协议与恢复 checkpoint 中的元数据一致。

任何已配置 gate 不满足时，部署都会 fail-closed，不会静默改变执行语义。

## 运行模型

受管通道提供以下保证：

- Reader command、split assignment、barrier、checkpoint callback、schema change、优雅关闭和取消
  进入同一个顺序域。
- 对每个已受管组件，专用 managed-command Hazelcast operation thread 只执行有界准入，不执行
  连接器代码，也不等待 command 完成。
- 已接收的 split assignment 会保留在引擎 assignment ledger 中，直到 Reader checkpoint
  inclusion 得到证明。
- 传输语义为 at-least-once，并使用 command 去重、attempt fencing、checksum 和显式协议版本。
- Reader poll 按 records、估算 bytes 和时间协作式让出，不把每条 record 复制到 mailbox。
- 每个受管 Reader 只复用一个 watchdog，watchdog 数量不随 records/sec 或 poll 次数增长。
- Coordinator 的阻塞 discovery 在引擎有界 I/O 或 CPU worker pool 中运行，结果回到 coordinator
  event loop 应用，旧 epoch 结果会被丢弃。
- Mailbox、outbound command、scheduler callback、async queue 和 assignment history 都有 count
  或 byte 硬上限，并为 barrier 与 terminal control 保留容量。

引擎不会自动删除旧连接器内部的锁。未通过受管契约认证的连接器继续走 Legacy 通道。
Capability 按组件生效：Iceberg coordinator-only 灰度只把 discovery 和 coordinator state 迁入受管
owner，Reader transport 与 Reader 锁仍明确保留在 Legacy 通道。只有 Reader 与 coordinator
capability 同时启用时，“operation thread 不执行 connector code”才是端到端保证。

## 开启灰度

在所有 Zeta 节点的 `config/seatunnel.yaml` 中配置：

```yaml
seatunnel:
  engine:
    managed-source-runtime:
      enabled: true
      connector-allowlist:
        - FakeSource
      runtime-protocol-version: 1
```

通道选择会冻结在 physical plan 中，并写入 checkpoint 元数据。配置热更新不会切换运行中作业
的执行通道。

首批认证试点为：

| 连接器 | 验证契约 |
|---|---|
| FakeSource | 受管 Reader poll、availability、split state 与 checkpoint 顺序 |
| Kafka | 后台 fetcher 隔离、异步 commit 回调、split discovery 与 assignment |
| Iceberg | 通过 coordinator scheduler 执行阻塞式流发现 |

只有白名单并不足以启用受管通道。连接器未声明兼容 capability 时，部署会直接失败。

## 安全配置

所有 byte 配置使用原始字节数，duration 配置使用毫秒。

| 配置项 | 默认值 | 作用 |
|---|---:|---|
| `enabled` | `false` | 是否允许新 physical plan 选择受管通道 |
| `connector-allowlist` | `[]` | 可进入受管通道的精确连接器插件名；拒绝通配符 |
| `runtime-protocol-version` | `1` | 引擎与连接器必须一致的协议版本 |
| `reader-mailbox-max-commands` | `1024` | 单 Reader command 总容量 |
| `reader-mailbox-max-bytes` | `4194304` | 单 Reader command 字节容量 |
| `reader-reserved-control-commands` | `64` | 为 checkpoint 和 terminal control 保留的 command |
| `reader-reserved-control-bytes` | `262144` | 为 checkpoint 和 terminal control 保留的字节 |
| `worker-mailbox-max-bytes` | `268435456` | Worker 级受管 Source 共享内存上限 |
| `max-command-payload-bytes` | `524288` | split/report 分片前的最大 payload |
| `poll-max-records` | `64` | 单次 poll turn 最大记录数 |
| `poll-max-bytes` | `1048576` | 单次 poll turn 最大估算输出字节数 |
| `poll-soft-duration-ms` | `5` | 协作式 poll deadline 和告警阈值 |
| `poll-hard-duration-ms` | `1000` | 触发 `wakeUp()` 的阈值 |
| `poll-cancellation-timeout-ms` | `30000` | 继续超时后失败并中断 task 的时间 |
| `idle-wait-ms` | `10` | Event loop 空闲等待时间 |
| `admission-budget-ms` | `5` | Operation thread 准入预算 |
| `retry-initial-backoff-ms` | `10` | 传输初始重试间隔 |
| `retry-max-backoff-ms` | `1000` | 最大重试间隔 |
| `command-retry-deadline-ms` | `30000` | Durable command 投递期限 |
| `coordinator-async-max-concurrency` | `4` | 单 coordinator 最大 async 并发 |
| `coordinator-async-io-threads` | `32` | Worker 级阻塞 discovery 线程数 |
| `coordinator-async-cpu-threads` | `4` | Worker 级 CPU discovery 线程数 |
| `coordinator-async-queue-capacity` | `4096` | 每个 Worker async queue 的容量 |
| `assignment-tracker-max-entries` | `100000` | 单 coordinator assignment ledger 条目上限 |
| `assignment-tracker-max-bytes` | `67108864` | 单 coordinator assignment ledger 字节上限 |

保留容量占满整个 mailbox、单个 payload 侵占保留控制容量等非法组合，会在配置解析阶段被拒绝。
保留 control command 容量还必须不小于 coordinator async 最大并发数，确保每个在途 worker
始终有一个可发布 terminal completion 的槽位。

## Checkpoint 与恢复

`ACCEPTED` 只表示接收方运行时在当前 attempt 中接管了 command 投递，不表示 command 已经进入
完成的 checkpoint。

Split assignment 按以下协议推进：

1. Coordinator 在发送前记录 `DISPATCHED`。
2. Reader 准入后，ledger 进入 `ADMITTED`。
3. Reader 按 sender sequence 应用 command，并返回 application proof。
4. Reader snapshot 保存连接器 split state、applied watermark、稳定 split ID、
   no-more-splits generation 和生命周期元数据。
5. Reader checkpoint report 将匹配的 assignment 推进到 `CHECKPOINT_INCLUDED`。
6. 只有 checkpoint complete 后，ledger 条目才允许压缩。

故障恢复使用最后一次完成的连接器状态与引擎元数据，旧 attempt 和旧 async work 会被 fencing。
Rescale 时先用完成 checkpoint 中的 split identity 对账，再把无法确认的 assignment 返回
Enumerator。关闭 checkpoint 的作业只保留 ledger 到 application proof，不能声称 checkpoint
durability。
只有并行度不变时才保留逐 Reader 的 no-more-splits 状态。发生 rescale 后只传播全局
end-of-input；旧 subtask 的局部结束状态会被丢弃，由新 Reader 与恢复后的 Enumerator 重新协商，
避免新 Reader 被提前结束。
分块 checkpoint report 和恢复 ownership proof 只有在同一 fenced group 的所有 chunk 到齐后才会
生效。累积的 split ID 统一计入 worker 内存预算，并在完成、checkpoint 结束、attempt 替换或
runtime 关闭时释放。

Schema-change checkpoint 使用显式状态机。Schema change 过程中收到 graceful close 时先锁存 close，
完成 schema checkpoint 后再进入 `DRAINING`。Abort、timeout 或协议不匹配会使 Source 失败。

## 监控

受管指标只使用稳定的 Source action 后缀。Command ID、split ID、table name 和完整异常文本不会成为
指标维度。

至少监控：

- `SourceManagedMailboxCommands`、`SourceManagedMailboxBytes` 和
  `SourceManagedMailboxOldestAgeMs`。
- `SourceManagedAdmissionTotal`、`SourceManagedAdmissionNs` 和
  `SourceManagedAdmissionBudgetExceededTotal`。
- `SourceManagedCommandQueueNs`、`SourceManagedCommandNs` 和
  `SourceManagedTransportRetryTotal`。
- `SourceManagedAssignmentEntries`、`SourceManagedAssignmentBytes`、
  `SourceManagedAssignmentOldestAgeMs` 以及各状态条目数。
- `SourceManagedAsyncRunning`、`SourceManagedAsyncWaiting`、
  `SourceManagedAsyncTimeoutTotal` 以及 async queue/execution 时间。
- `SourceManagedWakeupTotal` 和 `SourceManagedWakeupTimeoutTotal`。

如果 mailbox age 持续增长、稳态消耗 reserved capacity、assignment 在成功 checkpoint 后仍持续增长、
admission 超预算、async timeout 出现，或 wakeup timeout 非零，应停止扩大灰度。

## 生产准入

使用相同硬件、JVM 和 workload，对比受管通道与 Legacy 基线：

- 并行度：`1`、`16`、`128`、`512`。
- Record size：`128 B`、`1 KiB`、`16 KiB`。
- Split 数：并行度的 `1x`、`10x`、`100x`。
- Checkpoint interval：`10 s`、`60 s`。
- Command load：稳态、`10x` 突发和 mailbox 满后恢复。
- 运行时间：稳态至少 30 分钟、包含 failover 至少 2 小时、soak 至少 24 小时。

退出标准为：吞吐下降不超过 3%，CPU 与稳态 heap 增加不超过 5%，checkpoint p99 增加不超过
5% 或 100 ms（取较大值），admission p99 不超过 5 ms，每个已认证 managed component 内的
operation thread 不执行 connector callback，不突破任何配置容量，并且 24 小时 soak 中
assignment 不出现无界增长。Reader 与 coordinator 全量认证时必须端到端满足 operation-thread
标准。

## 灰度与回滚

1. 保持 `enabled: false`，先冻结 Legacy 基线。
2. 在非生产集群只开启 `FakeSource`，完成恢复、rescale 和 mailbox saturation 测试。
3. Kafka 或 Iceberg 通过 conformance 与性能 gate 后，只灰度一个真实 workload。
4. 每次只扩大一个连接器和一种 workload。
5. 触发停止条件时，立即禁止新部署选择受管通道。

不要把受管 checkpoint 恢复到 Legacy 通道。需要可恢复回滚时，应维持同一受管 selection 直到灰度
作业 drain 完成，或者从更早的兼容 Legacy checkpoint、savepoint 或初始 source position 启动。
运行中作业及其持久化 physical plan 的 failover，不会因为集群白名单变化而自动切换通道。

协议版本 1 明确拒绝任意自定义 `SourceEvent` payload。依赖该能力的连接器必须继续留在 Legacy
通道，直到引擎提供版本化 event codec。
