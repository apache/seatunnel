---
title: Core API Design
---

# 核心 API 设计

## 为什么需要这篇文档

SeaTunnel 现在已经有了 Source、Sink、CatalogTable、Translation Layer 等单独页面，但还缺一篇能把这些 API 作为一个整体讲清楚的总览页。

这篇文档补的就是这层桥接视角。

## 设计目标

SeaTunnel 核心 API 设计最重要的目标只有一句话：

**让 connector 开发者只表达一次数据集成逻辑，而执行引擎差异由下层去处理。**

为了做到这一点，API 层必须同时满足三件事：

- 对 connector 作者提供稳定契约
- 携带足够的元数据以支持校验和规划
- 尽量独立于 Flink、Spark、Zeta 的运行时细节

## API 分层

SeaTunnel 的 API 层大致可以理解为下面这个结构：

```text
用户配置
   |
   v
Option / OptionRule / ReadonlyConfig
   |
   v
Factory 层
  - TableSourceFactory
  - TableSinkFactory
  - Transform factory
   |
   v
运行时契约
  - SeaTunnelSource
  - SeaTunnelSink
  - SeaTunnelTransform
   |
   v
元数据契约
  - CatalogTable
  - TableSchema
  - SeaTunnelDataType
  - SchemaChangeEvent
   |
   v
Translation / Engine Runtime
```

核心点在于：connector 逻辑位于中间层，足够高，避免直接耦合引擎；又足够丰富，能表达真实的数据集成链路。

## 五类核心 API

### 1. 配置契约

在 connector 真正运行之前，SeaTunnel 需要一套稳定方式来描述用户可见参数并进行校验。

这部分主要围绕：

- `Option`
- `OptionRule`
- `ReadonlyConfig`

这一层的重要性在于，它直接连接了：

- 文档
- 运行时校验
- 插件发现元数据
- UI 或 REST 驱动的配置流程

相关文档：

- [配置与 Option 系统](./configuration-and-option-system.md)
- [作业配置指南](../getting-started/job-configuration-guide.md)

### 2. Source 契约

Source 侧的职责，是把外部系统转换成 `SeaTunnelRow` 记录流，并在需要时带上 schema 和 state 元数据。

核心接口包括：

- `SeaTunnelSource`
- `SourceSplitEnumerator`
- `SourceReader`
- `SourceSplit`

其设计重点是把协调和执行分开：

- enumerator 管理发现与分配
- reader 在 worker 侧真正读取数据

这样并行、故障恢复、checkpoint 才能成为统一能力，而不是每个 connector 自己发明一套执行模型。

相关文档：

- [Source 架构](./api-design/source-architecture.md)

### 3. Sink 契约

Sink 侧负责把处理后的行数据变成外部系统中的可见副作用。

核心接口包括：

- `SeaTunnelSink`
- `SinkWriter`
- `SinkCommitter`
- `SinkAggregatedCommitter`

它的设计目标不只是“把数据写出去”，而是要让 sink 能明确表达：

- writer 侧缓冲与 prepare 逻辑
- commit 协调方式
- retry 与幂等行为
- 与 checkpoint 驱动恢复的兼容性

相关文档：

- [Sink 架构](./api-design/sink-architecture.md)

### 4. Transform 契约

Transform 位于 source 和 sink 之间，它作用于 SeaTunnel 的行模型和表模型，而不是某个引擎原生 record。

这让 SeaTunnel 可以用统一契约表达：

- 字段映射
- 过滤
- SQL 风格投影
- 元数据增强
- 多表路由与转换

Transform 契约的意义，是让作业保持声明式，而不因底层执行引擎变化而重写逻辑。

相关文档：

- [Transform 插件体系](./transform-plugin-system.md)
- [Transforms 目录](../transforms)

### 5. 元数据契约

一个严肃的数据集成系统，不能只处理行数据，还需要一套可移植的元数据模型来表达：

- 表标识
- schema
- type
- constraint
- partition key
- schema change event

这就是下面这些类型的职责：

- `CatalogTable`
- `TableSchema`
- `Column`
- `SeaTunnelDataType`
- `SchemaChangeEvent`

这一层直接支撑：

- sink 侧 schema 校验
- 多表作业
- schema evolution
- 引擎无关的规划能力

相关文档：

- [CatalogTable 与元数据管理](./api-design/catalog-table.md)

## 这些 API 如何协同工作

一条典型 SeaTunnel 作业里，API 契约大致按这个顺序协同：

1. factory 解析配置并校验参数
2. 创建 source、transform、sink 实例
3. source 发布 `CatalogTable` 元数据
4. transform 保留或改写行数据与 schema 信息
5. sink 校验或推导写入侧的表契约
6. translation 或原生运行时把这些契约适配到具体执行引擎

也正因为有这层拆分，SeaTunnel 才能在不把 connector 作者绑死到某个引擎 API 的前提下，实现跨引擎复用。

## 为什么要这样拆

### 用户契约与运行时契约分离

用户配置的变化节奏通常慢于内部运行时细节。把 `Option` / `OptionRule` 与 reader / writer 运行逻辑分离，能让用户配置更稳定，同时允许执行内部持续演进。

### 运行时契约与元数据契约分离

行数据、schema、schema change 的生命周期并不相同。一个 connector 可能持续产出行数据，但 metadata 只偶尔变化。把这些契约拆开，会让系统更容易扩展，也更容易推理。

### 逻辑 API 与引擎翻译层分离

如果 connector 直接写在 Flink 或 Spark API 之上，那么每个 connector 都要维护多套引擎实现。SeaTunnel API 层存在的意义，就是避免这种重复。

相关文档：

- [转换层](./api-design/translation-layer.md)

## 贡献者在评审 API 变更时先问什么

当你新增或评审一个 API 相关变更时，建议先问这几个问题：

- 这是用户可见契约，还是仅运行时可见契约
- 它应该落在 `OptionRule`、运行时 API，还是元数据 API
- 它会影响所有引擎，还是只影响某个 engine adapter
- 这个 option 名是否已经稳定到可以作为公开契约
- 它是否保持了对 connector 作者和用户的兼容性

这些问题重要，是因为 API 漂移通常比实现漂移更难回收。

## 常见误解

### “只要 Source 和 Sink API 就够了”

并不够。没有 `CatalogTable`、`SeaTunnelDataType`、schema change event，connector 就无法用引擎无关的方式表达表元数据和 schema evolution。

### “Transform 只是行级字段映射”

不完全是。SeaTunnel 的 transform 在多表和 CDC 场景下，往往还要保留或改写元数据。

### “Translation 只是个适配层”

它当然是适配层，但它更是一个设计边界。它把 connector 作者和引擎内部实现隔开，限制了引擎特定行为向 connector 代码泄漏。

## 推荐阅读顺序

1. 先读本页，建立整体 API 地图
2. 再读 [配置与 Option 系统](./configuration-and-option-system.md)
3. 再读 [Source 架构](./api-design/source-architecture.md)
4. 再读 [Sink 架构](./api-design/sink-architecture.md)
5. 再读 [Transform 插件体系](./transform-plugin-system.md)
6. 再读 [CatalogTable 与元数据管理](./api-design/catalog-table.md)
7. 再读 [转换层](./api-design/translation-layer.md)
