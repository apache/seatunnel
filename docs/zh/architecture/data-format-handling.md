---
title: 数据格式处理
---

# 数据格式处理

## 为什么需要这篇文档

SeaTunnel 现在已经有 Avro、Protobuf、Debezium JSON、Canal JSON、Maxwell JSON 等具体格式文档，但还缺一篇从系统视角解释“格式处理在 SeaTunnel 里到底处于什么位置”的页面。

这篇文档补的就是这一层。

## 核心思路

SeaTunnel 会刻意把下面三件事分开：

- 外部存储或传输系统
- connector 运行时
- 记录序列化格式

例如，Kafka 是传输系统，Kafka connector 是运行时桥梁，而 Avro 或 Debezium JSON 才是格式。

这种拆分很重要，因为：

- 同一个 connector 可能支持多种格式
- 同一种格式也可能出现在多个 connector 中

## 格式在链路中的位置

格式通常位于 SeaTunnel 内部行模型和外部字节 / 消息表示之间的边界层。

典型链路如下：

```text
外部 bytes / messages
        |
        v
Connector reader
        |
        v
格式解码器
        |
        v
SeaTunnelRow + schema metadata
        |
        v
Transforms 与引擎运行时
        |
        v
SeaTunnelRow
        |
        v
格式编码器
        |
        v
外部 bytes / messages
```

所以格式处理不是“最后补一下的序列化细节”，它会直接影响 schema、类型转换、CDC 语义以及外部生态兼容性。

## SeaTunnel 内部模型长什么样

在 SeaTunnel 内部，数据通常围绕这些对象表达：

- `SeaTunnelRow`
- `CatalogTable`
- `TableSchema`
- `SeaTunnelDataType`

格式层负责把这些内部模型与外部表示桥接起来，例如：

- JSON 文档
- Avro 记录
- Protobuf 消息
- CDC envelope 格式

相关文档：

- [核心 API 设计](./core-api-design.md)
- [CatalogTable 与元数据管理](./api-design/catalog-table.md)

## 常见格式类别

### 普通行格式

这类格式主要表达字段值和结构本身，例如：

- JSON
- Avro
- Protobuf
- 通过文件 connector 使用的 text / CSV 风格格式

常见关注点：

- 字段顺序与命名
- nullable
- 数值与时间类型映射
- 是否带 schema

### CDC Envelope 格式

这类格式除了行值，还会携带变更语义，例如：

- Debezium JSON
- CDC-compatible Debezium JSON
- Canal JSON
- Maxwell JSON
- OGG JSON

常见关注点：

- row kind 映射
- before / after 镜像
- source metadata
- schema change 兼容性

相关文档：

- [CDC Pipeline 架构概览](./cdc-pipeline-architecture.md)

## 格式处理通常发生在哪里

### Source 侧

在 source 侧，格式处理通常负责把外部 payload 解码成 SeaTunnel 的内部行模型。

例如：

- 从 Kafka 读取 Avro 或 JSON
- 从文件 connector 读取 JSON 或文本记录
- 从消息系统中解码 CDC envelope

### Sink 侧

在 sink 侧，格式处理通常负责把行数据重新序列化成目标系统预期的 payload 结构。

例如：

- 向 Kafka 写入 Debezium-compatible 消息
- 向消息系统写入 Avro 或 Protobuf
- 写出 JSON 或文本文件

## 为什么格式处理不只是“序列化”

在 SeaTunnel 里，格式处理往往还决定了：

- schema 如何推断或校验
- 外部字段类型如何映射到 `SeaTunnelDataType`
- CDC metadata 是否被保留
- 目标生态是否能直接消费输出结果

因此，格式设计和类型系统设计、connector 可用性是强耦合的。

## Schema 与格式的关系

格式和 schema 的关系并不只有一种模式。

### Schema-Driven 格式

有些格式更适合在显式 schema 存在时使用，这个 schema 可以来自 connector、外部 registry，或者用户自己定义的 schema block。

典型例子：

- Avro
- Protobuf
- 强类型 JSON 流程

### Schema-Lite 或 Schema-Less 格式

有些格式在 schema 约束较弱时也能工作，但这会把更多责任转移到 connector 配置或运行时推断。

典型例子：

- 普通 JSON
- 文本型 payload

相关文档：

- [Schema Feature](../introduction/concepts/schema-feature.md)

## CDC 格式处理

CDC 格式尤其重要，因为它们常被用来把 SeaTunnel 接入既有生态。

常见使用动机包括：

- 保持与 Debezium 消费端兼容
- 把 changelog 事件发布到 Kafka
- 为下游保留 before / after 语义
- 把数据库 CDC 桥接到消息系统

在设计或评审一条 CDC 格式链路时，建议重点检查：

- row kind 是否被正确保留
- source metadata 是否被正确保留
- topic、table、key 约定是否对下游保持兼容

## 类型映射高风险区域

格式处理里最容易出问题的地方通常包括：

- decimal 精度与 scale
- timestamp 和时区语义
- 嵌套 row、map、array 类型
- binary 编码
- schema-less JSON 中的 null 处理

这些问题表面上常常看起来像 connector bug，但真正的边界问题往往发生在格式转换层。

## 运维视角下的常见问题

当格式处理出问题时，常见表现通常是：

- decode 失败
- schema 不兼容
- 下游消费者拒绝消息
- 精度丢失或意外类型转换

排查时建议先问这几个问题：

1. connector 使用的是否真的是预期格式
2. source 和 sink 是否对 schema 预期一致
3. 这是普通行格式还是 CDC envelope 格式
4. 嵌套字段和时间字段是否保持了一致映射

## 推荐阅读顺序

1. 先读本页，建立系统视角
2. 再读 [CatalogTable 与元数据管理](./api-design/catalog-table.md)
3. 再读 [Schema Feature](../introduction/concepts/schema-feature.md)
4. 再进入 `connectors/formats/` 下的一篇具体格式文档
5. 如果是 changelog 类格式，再读 [CDC Pipeline 架构概览](./cdc-pipeline-architecture.md)
