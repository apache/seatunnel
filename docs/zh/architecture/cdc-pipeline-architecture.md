---
title: CDC Pipeline Architecture
---

# CDC Pipeline 架构概览

## 为什么需要这篇文档

SeaTunnel 已经有不少 CDC connector 文档，用来说明某个具体 source 或 sink 怎么配置。但官网里仍然缺一篇“整条 CDC 链路”的官方总览，回答下面这些问题：

- 全量快照和增量同步是怎么衔接的
- 变更事件在 SeaTunnel 里是如何表示的
- schema 变化和表元数据如何沿链路传播
- checkpoint、状态恢复、sink 提交语义是怎样协同工作的

这篇文档补的就是这个视角。

## CDC 在 SeaTunnel 里的位置

CDC 作业本质上仍然是标准的 SeaTunnel 作业：

```text
Source -> Transform -> Sink
```

不同点在于，CDC Source 输出的不只是 append-only 数据，而是带变更语义的事件流。因此一条 CDC Pipeline 通常具备这些特点：

- Source 启动时往往要经历快照阶段
- 快照之后会切换到增量日志读取阶段
- 行数据会携带 RowKind 和来源元数据
- Pipeline 可能还会传播 schema change
- Sink 需要知道如何处理 insert、update、delete

更准确的理解方式是：CDC 不是一套完全独立的执行系统，而是构建在 Source API、checkpoint 和引擎调度机制之上的一种特化链路。

## 高层数据流

```text
数据库 / 日志源
        |
        v
CDC Source
  - 快照 split 发现
  - 增量 split 发现
  - offset 跟踪
        |
        v
SeaTunnelRow + RowKind + metadata
        |
        v
Transforms
  - 路由
  - 过滤
  - metadata 提取
  - rowkind 转换
        |
        v
CDC-aware Sink
  - upsert / delete 处理
  - 事务型或幂等型提交
```

## 核心构成

### 快照阶段 + 增量阶段

大多数关系型 CDC connector 不会直接从 changelog 开始消费，而是会先做一次一致性的全量快照，然后再衔接到增量日志。

通用模式通常是：

1. 先把大表拆成多个 snapshot chunk
2. 并行 reader 处理这些 chunk
3. 记录快照与增量之间的切换点
4. 再继续消费数据库日志或 change stream

这也是为什么 CDC source 的 enumerator、split state 往往比普通 batch source 更复杂。

### 行模型与变更语义

CDC 数据在链路中仍然通过 `SeaTunnelRow` 传播，但这时它通常不再默认等价于 append-only 行。它可能表示：

- insert
- update before / update after
- delete

有些 transform 和 sink 会直接保留这些语义；有些则会把它转换成 append-only 数据，再交给只支持追加写入的下游。

相关文档：

- [Catalog Table](./api-design/catalog-table.md)
- [RowKindExtractor](../transforms/rowkind-extractor.md)
- [Metadata](../transforms/metadata.md)

### 多表与 Schema Evolution

CDC 很多时候不是同步单表，而是整个库或多个表。这时 source 会输出多张表的数据、表标识以及 schema change 事件。

SeaTunnel 依赖表元数据来支持：

- 多表路由
- sink 侧表创建或表查找
- schema evolution 决策
- sink option 中的 placeholder 替换

相关文档：

- [多表支持](./features/multi-table.md)
- [Catalog Table](./api-design/catalog-table.md)
- [Schema Evolution 配置](../introduction/configuration/schema-evolution.md)

## 执行阶段

### 作业启动

作业启动时，SeaTunnel 会先解析配置、校验 connector 参数、发现插件，并生成 logical plan 与 physical plan。CDC 作业和普通作业在这一步没有本质区别。

### Snapshot Phase

在 snapshot phase，source enumerator 负责生成 snapshot split，reader 并行消费这些 split，并把每个 split 的进度写入状态。

这一阶段直接影响：

- 首次启动耗时
- 初始一致性边界
- 在 bootstrap 阶段发生故障时的恢复代价

### Incremental Phase

当 snapshot 完成后，source 会进入增量读取阶段。这时通常会维护数据库特定的 offset 或位置，比如 binlog position、GTID、LSN，或者其他 change-stream cursor。

### Sink 落地

Sink 需要决定怎样把变更事件真正落到目标系统。常见方式包括：

- 上游先把 CDC 事件转换成 append-only，再做普通写入
- 以主键为基础做 upsert
- 透传 delete
- 借助两阶段提交或幂等提交来配合 checkpoint

## Checkpoint 与恢复

对 CDC 来说，checkpoint 非常关键，因为系统需要同时恢复：

- source 侧的进度，包括 split state 与 offset state
- sink 侧的提交状态，如果该 sink 参与了 exactly-once 风格的提交协调

实际表现通常是：

- source 在 checkpoint 时快照 split 和增量 offset
- 引擎持久化 checkpoint 元数据
- 恢复时 reader 和 enumerator 基于最近一次成功 checkpoint 重建状态
- sink 如果支持，则恢复未完成或可重试的 commit 信息

相关文档：

- [Checkpoint 机制](./fault-tolerance/checkpoint-mechanism.md)
- [Exactly-Once](./fault-tolerance/exactly-once.md)

## Source 侧架构

SeaTunnel CDC source 仍然建立在统一的 Source API 之上，但一般会使用更复杂的 split 模型。

典型组件包括：

- `SeaTunnelSource`
- `SourceSplitEnumerator`
- `SourceReader`
- snapshot split state
- incremental split state
- 数据库方言或 offset 抽象

常见职责包括：

- 发现 snapshot chunk
- 分配与回收 split
- 跟踪已完成的 snapshot split
- 通过 checkpoint 持久化增量 offset
- 将 schema 和 metadata 暴露给下游

相关文档：

- [Source 架构](./api-design/source-architecture.md)
- [配置与 Option 系统](./configuration-and-option-system.md)

## Sink 侧架构

CDC sink 的关键不只是“能写”，而是要先把 update / delete 语义说清楚。设计或评审一个 CDC sink 时，建议先检查这些问题：

- sink 是否支持基于主键的 upsert
- delete 事件是透传、忽略，还是在上游转换
- commit 是否幂等
- 是否参与 checkpoint 驱动的提交
- schema change 如何处理

相关文档：

- [Sink 架构](./api-design/sink-architecture.md)
- [Sink Connectors](../connectors/sink)

## 运维侧关注点

CDC Pipeline 比 append-only batch 作业更敏感，常见运维关注点包括：

- 源端日志保留时间与消费延迟
- 大表快照耗时
- checkpoint 间隔与 checkpoint 大小
- sink commit 延迟
- schema evolution 兼容性
- 集群节点之间的插件依赖隔离

如果一条 CDC 作业运行异常，通常优先看这些地方：

- source connector 日志
- checkpoint 状态
- sink commit 日志
- REST API 或 Web UI 中的作业状态

相关文档：

- [REST API and Web UI](../engines/zeta/rest-api-and-web-ui.md)
- [插件发现与类加载](./plugin-discovery-and-class-loading.md)
- [Connector 依赖隔离加载机制](../connectors/connector-isolated-dependency.md)

## 代码入口

如果你想直接看实现，而不是只看文档，建议从这些目录开始：

- `seatunnel-connectors-v2/connector-cdc/connector-cdc-base/`
- `seatunnel-connectors-v2/connector-cdc/connector-cdc-mysql/`
- `seatunnel-connectors-v2/connector-cdc/connector-cdc-postgres/`
- `seatunnel-connectors-v2/connector-cdc/connector-cdc-sqlserver/`
- `seatunnel-connectors-v2/connector-cdc/connector-cdc-oracle/`
- `seatunnel-connectors-v2/connector-cdc/connector-cdc-mongodb/`

通常最值得先看的类是：

- `IncrementalSource`
- `IncrementalSourceEnumerator`
- `HybridSplitAssigner`
- `SnapshotSplitAssigner`
- `IncrementalSourceReader`

## 推荐阅读顺序

1. 先读本页，建立整条链路视图
2. 再读 [Source 架构](./api-design/source-architecture.md)
3. 再读 [Catalog Table](./api-design/catalog-table.md)
4. 再读 [Checkpoint 机制](./fault-tolerance/checkpoint-mechanism.md)
5. 最后结合一篇具体 CDC connector 文档，例如 MySQL、Postgres 或 SQL Server
