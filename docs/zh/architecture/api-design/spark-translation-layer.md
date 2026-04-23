---
title: Spark 转换层
---

# Spark 转换层

## 目的

这篇文档解释 SeaTunnel 是如何把 connector API 适配到 Apache Spark 的。

如果你需要先看整体结构，建议先读 [转换层](./translation-layer.md)。本页聚焦 Spark 这条具体路径。

## 为什么 Spark 适配和 Flink 不一样

Spark 并不只是“另一个长得和 Flink 很像的引擎”。它的执行模型、datasource 接口、writer / commit 生命周期都有明显差异。

这意味着 Spark 转换层做的事情不只是换一层接口名称，而是要把 SeaTunnel 契约重新解释成 Spark 可接受的执行模型。

## 主要设计目标

Spark 转换层要尽量保留 SeaTunnel 语义，同时把它映射到 Spark 原生概念中，例如：

- datasource reader
- input partition
- internal row
- datasource writer 与 commit message

关键点不是把 Spark 伪装成 Flink，而是让 connector 作者不需要直接承担 Spark 侧复杂性。

## 高层映射关系

从概念上看，Spark 路径大致如下：

```text
SeaTunnelSource -> Spark source adapter -> Spark datasource runtime
SeaTunnelSink   -> Spark sink adapter   -> Spark datasource writer runtime
SeaTunnel schema/types -> Spark schema/types -> InternalRow execution
```

这条适配路径里最重要的三类问题是：

- source partition 规划
- row 与 schema 转换
- sink commit / abort 行为

## Source 侧适配

在 source 侧，Spark 转换层通常需要完成这些事情：

- 以 Spark 期望的形式暴露 schema
- 从 SeaTunnel split 规划 Spark partitions
- 为每个 partition 创建 reader
- 把 SeaTunnel 输出转换成 Spark `InternalRow`

和 Flink 相比，Spark 更强调“规划完成的 partitions + reader 执行”，而不是长期存在的 enumerator / coordinator 运行模型。

这直接影响了 adapter 的设计方式。

相关文档：

- [Source 架构](./source-architecture.md)
- [表模型与类型系统](../table-schema-and-type-system.md)

## Sink 侧适配

在 sink 侧，Spark 转换层要把 SeaTunnel sink 语义映射到 Spark datasource writer 契约。

典型职责包括：

- 创建 writer factory
- 在 executor 与 driver 之间传递 commit message
- 协调 commit 与 abort 路径
- 把 SeaTunnel 的 retry 语义映射到 Spark 可接受的行为

当 sink 不是 append-only，而是依赖幂等或事务提交时，这一点尤其关键。

相关文档：

- [Sink 架构](./sink-architecture.md)
- [Exactly-Once](../fault-tolerance/exactly-once.md)

## Schema 与 Row 转换

Spark 转换层对 schema 转换非常敏感，因为 Spark 本身依赖严格的 row / schema 模型执行。

因此转换层必须把下面这些对象映射成 Spark 侧对象：

- `CatalogTable` / `TableSchema`
- `SeaTunnelDataType`
- `SeaTunnelRow`

对应到 Spark 的：

- `StructType`
- Spark SQL data type
- `InternalRow`

这也是 Spark 路径里最容易出问题的边界之一，尤其集中在：

- decimal
- timestamp
- 嵌套类型
- nullability

## 版本拆分

SeaTunnel 的 Spark 转换层是按版本线拆分的。Spark 2.4 和 Spark 3.x 的 datasource API 不完全一致，因此仓库里保留了不同 major 版本的独立适配模块。

这很重要，因为一个 connector 可能在 SeaTunnel API 层完全正确，但底层仍然需要针对具体 Spark 版本做差异化适配。

## Commit 与恢复关注点

Spark sink 执行有自己的一套 writer / commit message 模型。转换层必须把 SeaTunnel writer / committer 语义映射进去，同时不能丢失：

- 幂等要求
- 失败处理行为
- abort 正确性
- connector 对外承诺的一致性语义

如果这层桥接有问题，用户常见感知会是：

- 外部副作用重复
- abort 路径失效
- writer commit 不一致

## 常见问题集中区

Spark 转换层的问题通常集中在：

- schema 转换不一致
- `InternalRow` 转换
- datasource writer commit 行为
- Spark 2.4 与 Spark 3.x 适配器差异

这类问题经常会伪装成 connector bug，但真正的问题其实在转换层内部。

## 代码入口

建议从这些目录开始看实现：

- `seatunnel-translation/seatunnel-translation-spark/`
- `seatunnel-translation/seatunnel-translation-spark/seatunnel-translation-spark-common/`
- `seatunnel-translation/seatunnel-translation-spark/seatunnel-translation-spark-2.4/`
- `seatunnel-translation/seatunnel-translation-spark/seatunnel-translation-spark-3.3/`

可以优先关注这些类：

- Spark source provider 相关实现
- `SparkSink`
- `SparkDataSourceWriter`
- `SeaTunnelInputPartitionReader`

## 推荐阅读顺序

1. [转换层](./translation-layer.md)
2. 本页
3. [Source 架构](./source-architecture.md)
4. [Sink 架构](./sink-architecture.md)
5. [表模型与类型系统](../table-schema-and-type-system.md)
