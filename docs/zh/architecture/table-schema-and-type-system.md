---
title: Table Schema and Type System
---

# 表模型与类型系统

## 为什么需要这篇文档

SeaTunnel 现在已经有 schema 配置文档，也有 `CatalogTable` 元数据文档，但还缺一篇从系统视角解释“表模型”和“类型系统”如何贯穿 connector、transform、sink 和多引擎运行时的总览页。

这篇文档补的就是这一层。

## 核心思路

SeaTunnel 需要一套 schema 与 type 模型，同时满足下面这些要求：

- 能足够准确地描述真实数据集成里的外部表和记录
- 不依赖某一个执行引擎
- 既支持静态 schema，也支持运行时 schema evolution
- 能同时被 source、transform、sink 使用

这套模型主要围绕这些对象展开：

- `CatalogTable`
- `TableSchema`
- `Column`
- `SeaTunnelDataType`
- `SchemaChangeEvent`

## Schema 在 SeaTunnel 作业里的位置

Schema 不是只有 source 才关心的事情，它会贯穿整条 pipeline。

```text
用户配置或源端发现
        |
        v
CatalogTable / TableSchema
        |
        v
Source 输出契约
        |
        v
Transform 规划与校验
        |
        v
Sink 兼容性与写入契约
        |
        v
引擎翻译层与运行时执行
```

这也是为什么 schema 出问题时，表面上可能出现在 connector、transform 或 sink 的不同阶段。

## 主要构件

### CatalogTable

`CatalogTable` 是顶层元数据对象，负责承载表标识以及 pipeline 需要的 schema 信息。

它通常会包含：

- table identifier
- table schema
- options
- partition keys
- comment 等补充元数据

### TableSchema

`TableSchema` 描述的是一张表的逻辑结构：

- columns
- primary key
- constraint keys

它是 connector 和 transform 在需要表达“表结构”时使用的核心契约。

### Column

每个列对象通常会提供：

- name
- type
- nullability
- default value
- comment

### SeaTunnelDataType

`SeaTunnelDataType` 是 SeaTunnel 的可移植类型系统。它的作用，是让 connector 能描述字段类型，而不把这种描述直接绑定到 Flink、Spark、JDBC、Avro 或某个数据库方言上。

## 为什么必须有独立类型系统

SeaTunnel 面对的是大量异构系统。一条作业很可能是从一种类型系统读，再写到另一种类型系统：

- JDBC 类型
- Kafka / Avro 类型
- JSON payload
- 文件 schema
- CDC metadata 与 row kind

如果 SeaTunnel 不先把这些都归一到自己的类型模型里，那么 transform 和 sink 就都要同时理解引擎差异和 connector 差异，系统会迅速碎片化。

独立类型系统就是为了解决这个问题。

## 常见类型类别

SeaTunnel 的类型系统必须支持的不只是 primitive value。

### 基础类型

典型基础类型包括：

- string
- boolean
- tinyint / smallint / int / bigint
- float / double / decimal
- bytes
- date / time / timestamp / 在支持场景下的带时区时间语义

### 复杂类型

为了支撑半结构化数据和 CDC payload，schema 模型还必须支持嵌套结构，例如：

- array
- map
- row

这很重要，因为现代数据集成链路很少从头到尾都是纯平面结构。

## Schema 从哪里来

在 SeaTunnel 里，schema 可以有多种来源。

### Source 发现的 Schema

有些 connector 能直接从外部系统获取 schema，例如：

- 关系型数据库
- catalog
- metadata service

### 用户声明的 Schema

有些系统本身没有强 schema，或者用户希望覆盖、补充 schema。这时 SeaTunnel 支持用户显式配置 schema。

相关文档：

- [Schema Feature](../introduction/concepts/schema-feature.md)

### 传播或派生出的 Schema

Transform 可能会保留上游 schema，也可能裁剪字段、重命名字段、生成新字段或派生新 schema。

这意味着 schema 并不是“只在 source 阶段读一次”的东西，而是作业逻辑契约的一部分。

## Schema 在 Source、Transform、Sink 中的作用

### Source 侧

Source 使用 schema 来定义下游应该接收到什么。

常见用途包括：

- 产出 `CatalogTable`
- 表达表标识
- 暴露多表元数据
- 把外部类型映射到 `SeaTunnelDataType`

### Transform 侧

Transform 可能会：

- 保持 schema 不变
- 投影部分字段
- 重命名列
- 生成新列
- 映射 schema change event

也就是说，transform 不只是行级逻辑，很多时候也是 schema 逻辑。

### Sink 侧

Sink 使用 schema 来判断当前输入是否可以安全写入目标端。

常见检查包括：

- 字段是否存在
- 类型是否兼容
- 是否满足主键要求
- 分区元数据是否满足
- schema evolution 怎么处理

## 多表与 CDC 场景下为什么更重要

Schema 在下面两类作业里尤其关键。

### 多表作业

当 source 一次输出多张表时，SeaTunnel 必须依赖稳定的表标识和 schema 模型，才能保证路由和 sink 落地正确。

### CDC 作业

在 CDC pipeline 中，schema 不是静态的。系统可能需要在运行时传播 schema change，因此 schema 处理天然和这些能力绑定：

- `SchemaChangeEvent`
- checkpoint 与恢复
- sink 侧兼容性逻辑

相关文档：

- [CDC Pipeline 架构概览](./cdc-pipeline-architecture.md)
- [CatalogTable 与元数据管理](./api-design/catalog-table.md)

## 类型映射的高风险边界

类型系统最容易出问题的地方，通常都发生在系统边界上。

高风险区域包括：

- decimal 的 precision / scale
- timestamp 语义与时区解释
- 嵌套 row / map / array 兼容性
- binary 表示
- nullability 假设

这些地方往往意味着“能编译通过”并不等于“可以安全上生产”。

## Schema Evolution

一套有用的 schema 系统，既要支持稳定，也要支持受控变化。

SeaTunnel 的 schema 模型支持这类演化相关流程：

- add column
- drop column
- modify column
- 向下游传播 schema change

这在下面这些场景中特别重要：

- CDC pipeline
- 湖仓一体写入
- 长时间运行且 schema 会变化的作业

相关文档：

- [Schema Evolution 配置](../introduction/configuration/schema-evolution.md)

## 推荐阅读顺序

1. 先读本页，建立系统视角
2. 再读 [CatalogTable 与元数据管理](./api-design/catalog-table.md)
3. 再读 [Schema Feature](../introduction/concepts/schema-feature.md)
4. 再读 [配置与 Option 系统](./configuration-and-option-system.md)
5. 如果预期运行时 schema 会变化，再读 [CDC Pipeline 架构概览](./cdc-pipeline-architecture.md)
