---
title: CDC Schema Evolution 契约
---

# CDC Schema Evolution 契约

## 文档目标

本文定义启用 `SchemaChangeEvent` 后，CDC schema evolution 链路必须满足的端到端契约。它不是
单个 connector 的功能清单，而是 source、transform、engine、sink 共同遵守的运行时契约。

任何声称支持 schema evolution 的路径，都必须满足这份契约。无法满足契约的路径，必须在进入
含义不明确的数据流处理前显式失败，或者明确记录 connector 级限制。

## 范围

第一个版本按表定义。同一张表内必须保持自己的顺序与恢复状态；不同表的 schema change 可以
独立推进。跨表原子 DDL 不属于这个版本，connector 文档也不能暗示具备跨表原子能力。

支持的变更类型和 connector 矩阵见
[Schema Evolution 配置](../introduction/configuration/schema-evolution.md)。本文定义这些事件周围的
排序、checkpoint、sink apply、重放和恢复语义。

## 契约原语

### 表标识

每个 schema change event 都必须携带运行时和 sink 使用的稳定表标识。表标识是排序、协调、
sink apply、checkpoint 状态和恢复的 key。

### Schema epoch

对同一张表来说，每个 schema change 都必须有单调递增的 epoch，或者等价的可持久化事件标识。
epoch 用来去重重放事件，并判断恢复后 sink 是否已经应用过同一个 schema change。

如果某个 source 或 engine 路径无法提供稳定事件标识，就不能声明支持可恢复的 schema evolution。

### Schema 边界

对同一张表来说，schema change 会形成硬边界：

```text
old-schema records -> SchemaChangeEvent(epoch=N) -> new-schema records
```

使用新 schema 的记录不能越过 schema change event。使用旧 schema 的记录也不能在 sink 已经进入
新 epoch 后再被释放，除非 connector 显式编码旧 epoch 记录，并且 sink 能同时处理两个 epoch。

## 端到端流程

支持 schema evolution 的目标路径如下：

1. source 观察到 DDL，并转换成 `SchemaChangeEvent`。
2. source 关闭该表的 old-schema 数据前缀，并在释放 new-schema 数据前记录可持久化边界。
3. 每个 transform 通过 `SeaTunnelTransform.mapSchemaChangeEvent` 映射事件，并在处理 new-schema
   行之前刷新下游 catalog 状态。
4. engine 等待 old-schema 数据完成必须的 flush 或 commit，并越过要求的 checkpoint 边界。
5. engine 把事件发送给拥有该表的每个 sink subtask。
6. 每个 sink subtask 应用 DDL，或者上报显式失败。部分成功不能当作成功。
7. engine 在 checkpoint 状态中记录已应用 epoch，然后才把 new-schema 数据流作为可恢复状态继续释放或提交。

## 排序规则

对每一张表：

- schema change 按 source 观察顺序处理
- 同一时刻只能有一个 schema change 处于 active apply 窗口
- new-schema 记录必须等待 schema event，直到所有要求的 sink subtask 完成 apply
- 重放的 schema event 必须按 epoch 去重，或者以可操作错误失败
- 不支持的 transform 或 sink 必须在丢弃、乱序或静默忽略事件前失败

对不同表：

- 第一个版本允许不同表的 schema change 独立推进
- 某张表被阻塞时，不能静默打乱另一张表的记录顺序
- 如果 connector 在多张表之间共享同一个物理 sink 事务，必须说明这个事务是否扩大了协调边界

## 恢复状态机

engine 或运行时集成必须能为每张表的每个 epoch 区分这些状态。

| 状态 | 含义 | 恢复规则 |
| --- | --- | --- |
| `OBSERVED` | source 已观察到 schema change，但 old-schema 边界尚未持久化。 | 从最近一次 checkpoint 恢复，并从 source offset 重新读取事件。sink 不应收到 DDL。 |
| `BOUNDARY_DURABLE` | 边界前的 old-schema 记录已 checkpoint，或者已经以其他方式安全落地。 | 先应用 schema change，再释放 new-schema 记录。 |
| `APPLYING` | 一个或多个 sink subtask 正在应用 DDL。 | 只能通过幂等 sink apply 路径重试；否则必须校验外部 schema，无法判断结果时显式失败。 |
| `APPLIED_NOT_DURABLE` | 外部 sink schema 已改变，但运行时 epoch 状态尚未 checkpoint。 | 恢复时检测已应用 epoch 并补全运行时状态；如果 sink 无法证明已应用 schema，则在写入数据前失败。 |
| `EPOCH_DURABLE` | sink apply 结果和运行时 epoch 都已持久化。 | 可以从恢复后的 checkpoint 继续 new-schema 记录；同 epoch 重放事件按重复事件处理。 |

## 故障规则

以下故障点必须是确定性的：

| 故障点 | 必须行为 |
| --- | --- |
| 边界 checkpoint 前 | 从上一次 source checkpoint 重放。sink 不能已经收到 DDL。 |
| 边界 checkpoint 后、sink apply 前 | 先应用 DDL，再释放 new-schema 记录。 |
| sink apply 过程中 | 幂等重试、完成所有 subtask，或者让作业失败。部分 apply 必须能在错误里看出来。 |
| sink apply 后、下一次 checkpoint 前 | 恢复时检测已应用 epoch；如果 sink 无法证明 DDL 是否成功，则 fail fast。 |
| restore 过程中 | 在任何 new-schema 记录发出前，重建 source offset、transform catalog 状态、sink epoch 状态和 pending event。 |
| source、transform、engine 或 sink 不支持 | 在含义不明确的处理发生前失败。跳过或静默忽略事件不满足 schema evolution 契约。 |

## 组件职责

### Source

source 负责输出包含表标识、稳定 epoch、变更后 schema 的 schema change，并给出区分 old-schema 与
new-schema 记录的 checkpoint 边界。当前 source API 通过
`Collector.markSchemaChangeBeforeCheckpoint`、`Collector.collect(SchemaChangeEvent)` 和
`Collector.markSchemaChangeAfterCheckpoint` 暴露这个边界。

### Transform

如果 transform 会改变表标识、列名、列顺序或行结构，它必须用和后续数据行一致的方式映射 schema
change event。无法安全映射事件时，transform 必须失败，而不是把陈旧元数据继续传给下游。

### Engine

engine 负责对每张表的 schema epoch 做串行化处理，等待要求的 checkpoint 边界，把事件广播给所有
需要的 sink subtask，收集成功或失败，并 checkpoint 已应用 epoch。

### Sink

声称支持 schema evolution 的 sink 必须通过
`SupportSchemaEvolutionSinkWriter.applySchemaChange` 提供幂等或可验证的 DDL apply 路径。遇到不支持的
变更类型或部分 apply 失败时，必须抛出带表和 epoch 上下文的 task-failing error。

## 验证要求

任何声称实现该契约的代码，都应包含以下 E2E 覆盖：

- 连续 add、drop、rename、modify column 事件
- schema-change 边界 checkpoint 前失败
- 边界 checkpoint 后、sink apply 前失败
- sink apply 过程中失败，包括部分 subtask 成功
- sink apply 后、下一次 checkpoint 完成前失败
- 从 schema change 前后的 checkpoint 恢复
- 多张表独立发生 schema change
- sink 不支持目标 schema change 时显式失败

至少一个正向路径应使用 MySQL CDC 到支持 schema evolution 的 JDBC sink；至少一个负向路径应使用
不支持的 sink，并断言明确失败。

## 后续实现区域

这份契约应拆成聚焦的后续实现：

- API/event metadata：稳定的 per-table epoch、序列化兼容、transform 映射预期
- engine coordination：可恢复的 per-table 状态机、sink subtask ack、timeout、restore 和 replay 处理
- E2E recovery：覆盖以上每个故障点的故障注入，包括重复 DDL 和 unsupported-sink 路径

## 代码入口

修改代码时，建议从这些入口开始：

- `seatunnel-api/src/main/java/org/apache/seatunnel/api/table/schema/event/SchemaChangeEvent.java`
- `seatunnel-api/src/main/java/org/apache/seatunnel/api/source/Collector.java`
- `seatunnel-api/src/main/java/org/apache/seatunnel/api/transform/SeaTunnelTransform.java`
- `seatunnel-api/src/main/java/org/apache/seatunnel/api/sink/SupportSchemaEvolutionSinkWriter.java`
- `seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/debezium/row/SeaTunnelRowDebeziumDeserializeSchema.java`
- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/flow/SourceFlowLifeCycle.java`
- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/flow/SinkFlowLifeCycle.java`
- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/checkpoint/CheckpointCoordinator.java`
- `seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/src/main/java/org/apache/seatunnel/translation/flink/schema/SchemaOperator.java`
- `seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/src/main/java/org/apache/seatunnel/translation/flink/schema/BroadcastSchemaSinkOperator.java`
