---
sidebar_position: 1
title: 转换层
---

# 转换层架构

## 1. 概述

### 1.1 问题背景

SeaTunnel 提供统一的连接器 API,但作业需要在不同的执行引擎上运行:

- **引擎多样性**: Flink、Spark、SeaTunnel Engine (Zeta) 具有不同的 API
- **代码重复**: 没有转换,每个连接器需要 3 个实现
- **维护负担**: Bug 修复需要在所有实现中进行更改
- **API 演化**: 引擎 API 变更会破坏连接器
- **用户体验**: 用户希望跨引擎的一致行为

### 1.2 设计目标

SeaTunnel 的转换层旨在:

1. **实现可移植性**: 相同的连接器可在任何引擎上运行
2. **隐藏复杂性**: 连接器开发者只需学习 SeaTunnel API
3. **保持保真度**: 跨引擎保留语义保证
4. **最小化开销**: 尽量降低转换对吞吐/延迟的影响（取决于 connector、类型转换与引擎实现）
5. **支持演化**: 将连接器与引擎 API 变更隔离

### 1.3 架构概览

```
┌──────────────────────────────────────────────────────────────┐
│                   SeaTunnel API 层                            │
│         (引擎独立的连接器接口)                                │
│                                                                │
│  SeaTunnelSource    SeaTunnelSink    SeaTunnelTransform      │
└──────────────────────────────────────────────────────────────┘
                              │
                              │ 转换层
                ┌─────────────┼─────────────┐
                ▼             ▼             ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  Flink 适配器    │  │  Spark 适配器    │  │ Zeta (原生)      │
│                  │  │                  │  │                  │
│ FlinkSource      │  │ SparkSource      │  │ 直接             │
│ FlinkSink        │  │ SparkSink        │  │ 执行             │
└──────────────────┘  └──────────────────┘  └──────────────────┘
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  Apache Flink    │  │  Apache Spark    │  │ SeaTunnel Engine │
│     运行时       │  │     运行时       │  │      (Zeta)      │
└──────────────────┘  └──────────────────┘  └──────────────────┘
```

## 2. Flink 转换层

### 2.1 FlinkSource 适配器

将 `SeaTunnelSource` 适配到 Flink 的 `Source` 接口。

**适配点（语义级）**：
- **有界/无界语义**：把 SeaTunnel 的 boundedness 映射到 Flink 的 `Boundedness`
- **Reader 创建**：把 Flink `SourceReaderContext` 适配为 SeaTunnel reader context，并用 wrapper 把 SeaTunnel reader 包装成 Flink reader
- **Enumerator 创建**：把 Flink `SplitEnumeratorContext` 适配为 SeaTunnel enumerator context，并包装成 Flink enumerator
- **序列化器**：把 SeaTunnel 的 split/state 序列化器适配到 Flink 的 `SimpleVersionedSerializer`

### 2.2 FlinkSourceReader 适配器

**适配点（语义级）**：
- `start/open`：把 Flink 的 reader 生命周期委托给 SeaTunnel reader
- `pollNext`：把 Flink `ReaderOutput` 适配为 SeaTunnel collector，并映射“有无数据可读”的返回语义
- `addSplits`：把 Flink 的 split wrapper 解包为 SeaTunnel split 再下发
- `snapshotState`：把 SeaTunnel reader 的快照结果包装为 Flink 侧可序列化的 split/state
- `notifyCheckpointComplete`：把 checkpoint 完成通知下沉到 SeaTunnel reader（用于清理/提交等）

### 2.3 FlinkSourceEnumerator 适配器

**适配点（语义级）**：
- 生命周期：Flink enumerator 的 `start` 驱动 SeaTunnel enumerator 的 open/run
- 分片请求：Flink 的 split request 透传给 SeaTunnel enumerator 的分片分配逻辑
- 分片回退：把回退 split 解包并回交给 SeaTunnel enumerator
- 状态快照：把 enumerator state 包装成 Flink 可持久化的 wrapper，以参与 checkpoint

### 2.4 上下文适配器

**FlinkSourceReaderContext**:

- 下标与并行度：把 Flink 的 subtask index 映射为 SeaTunnel reader 的 index
- 事件通道：把 SeaTunnel 的 SourceEvent 包装后发送到 Flink 的 coordinator/event channel
- 分片请求：Flink 会在运行时自动触发 split request，SeaTunnel 侧通常不需要显式触发

**FlinkSourceSplitEnumeratorContext**:

- 并行度/注册 reader：把 Flink 的 runtime 信息暴露给 SeaTunnel enumerator
- 分片分配：把 SeaTunnel split 包装为 Flink split 并通过 Flink 的 assignment API 下发
- no-more-splits：在有界场景下通知 reader 结束
- 事件下发：把 SeaTunnel event 包装为 Flink event 并发送给指定 reader

### 2.5 FlinkSink 适配器

**适配点（语义级）**：
- writer：把 Flink `InitContext` 适配为 SeaTunnel writer context 并创建 SeaTunnel `SinkWriter`
- committer/global committer：把 SeaTunnel 的两阶段提交组件包装为 Flink 的 committer 体系
- serializer：把 SeaTunnel 的 commitInfo / writerState 序列化器适配为 Flink `SimpleVersionedSerializer`

### 2.6 FlinkSinkWriter 适配器

**适配点（语义级）**：
- `write`：把 Flink sink writer 的写入请求委托给 SeaTunnel `SinkWriter.write`
- `prepareCommit`：把 SeaTunnel `prepareCommit()` 的可选 commitInfo 映射为 Flink 的 committable 列表
- `snapshotState`：直接使用 SeaTunnel writer 的快照结果参与 Flink checkpoint
- `close`：委托关闭，确保释放外部资源

## 3. Spark 转换层

### 3.1 SparkSource 适配器

将 `SeaTunnelSource` 适配到 Spark 的数据源接口（Spark 2.4 与 Spark 3.x 使用的 DataSource API 形态不同，具体以对应版本适配模块实现为准）。

**适配点（语义级）**：
- `readSchema`：把 SeaTunnel `CatalogTable/TableSchema` 映射为 Spark `StructType`
- `planInputPartitions`：在 Spark 的批处理模型下，通常一次性生成全部 splits，并为每个 split 构造一个 `InputPartition`

Spark 的执行模型偏“批式规划”，因此枚举器的职责更像是“规划阶段生成分片集合”，而不是长期运行的调度器。

### 3.2 SparkInputPartition

**适配点（语义级）**：
- 每个 `InputPartition` 绑定一个 SeaTunnel split
- `createPartitionReader` 创建 SeaTunnel reader，注入该 split，并把输出转换为 Spark `InternalRow`

### 3.3 SparkPartitionReader

**适配点（语义级）**：
- 初始化：创建并打开 SeaTunnel reader，下发 split
- 读取循环：从 SeaTunnel reader 拉取记录并转换为 Spark `InternalRow`（必要时使用缓冲队列适配 pull-based API）
- 资源释放：关闭 reader 并释放外部资源

### 3.4 SparkSink 适配器

**适配点（语义级）**：
- writer factory：在 executor 侧创建写入器实例并接收 Spark `InternalRow`
- commit coordinator：当目标端存在提交器时启用 Spark 的提交协调路径
- commit/abort：把 Spark 的提交消息转换为 SeaTunnel 的 commitInfo 列表，并交由 SeaTunnel `SinkCommitter` 执行（要求幂等/可重试）

## 4. 序列化适配器

### 4.1 FlinkSimpleVersionedSerializer

**适配点（语义级）**：
- 版本：将 SeaTunnel serializer 的版本号透传到 Flink 侧
- 序列化/反序列化：直接委托给 SeaTunnel serializer，以保证跨引擎一致的状态编码

## 5. 类型转换

### 5.1 Spark 类型转换

**适配点（语义级）**：
- Schema：将 SeaTunnel `TableSchema` 映射为 Spark `StructType`
- DataType：按 `SqlType` 做一一映射（整数/浮点/decimal/string/boolean/date/timestamp/bytes/array/map 等）
- 兼容性：当引擎侧类型更细分时（例如 timestamp 语义差异），以 SeaTunnel 的“最小公分母”语义为准，并允许通过配置选择具体映射策略

## 6. 性能考虑

### 6.1 转换开销

转换层带来的开销主要来自上下文包装、类型转换、序列化/反序列化等。实际开销高度依赖具体 connector 的 I/O 特性与数据类型分布，因此本文不提供固定比例或吞吐数字，避免与真实环境产生偏差。

### 6.2 优化技术

**批量类型转换**:

- 优先批量转换（向量化/批处理）以摊销 per-row 转换成本
- 在不改变语义的前提下减少对象创建与复制（降低 GC 压力）

**避免不必要的包装**:

- 优先复用已有序列化能力，避免重复 wrapper 造成的额外拷贝
- 在必须 wrapper 时采用惰性策略：仅在 checkpoint/网络传输时做包装

## 7. 限制和解决方法

### 7.1 引擎特定功能

**问题**: 某些引擎功能在 SeaTunnel 中没有等效项。

**示例**: Flink 的 `WatermarkStrategy`

Flink 的 watermark/事件时间语义属于引擎特性，SeaTunnel 的连接器 API 默认不直接暴露该能力。

**解决方法**: 提供引擎特定配置
```hocon
source {
  Kafka {
    # SeaTunnel 配置
    topic = "my_topic"

    # 引擎特定配置(仅用于 Flink)
    flink.watermark.strategy = "bounded-out-of-orderness"
    flink.watermark.max-out-of-orderness = "5s"
  }
}
```

### 7.2 类型系统差异

**问题**: 类型系统不完全对齐。

**示例**: Spark 有 `TimestampType`,Flink 有 `LocalZonedTimestampType` 和 `TimestampType`。

**解决方法**: 使用最小公分母

SeaTunnel 侧使用统一抽象类型；转换层根据引擎能力与用户配置决定映射到哪一种引擎类型。

## 8. 最佳实践

### 8.1 连接器开发

**应该做的**:
- 仅实现 SeaTunnel API
- 在多个引擎上测试
- 使用 SeaTunnel 类型

**不应该做的**:
- 在连接器代码中引用引擎特定 API
- 假设特定引擎行为
- 使用引擎特定优化

### 8.2 测试

**在所有引擎上测试**:

- 建议使用参数化/矩阵测试：同一套连接器用例在 Flink/Spark/Zeta 上跑
- 覆盖语义一致性：exactly-once、checkpoint 恢复、schema 兼容、分片重新分配等

## 9. 相关资源

- [数据 Source 架构](../api-design/source-architecture.md)
- [目标端 Sink 架构](../api-design/sink-architecture.md)
- [设计理念](../design-philosophy.md)
