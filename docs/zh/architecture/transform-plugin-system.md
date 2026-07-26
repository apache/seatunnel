---
title: Transform 插件体系
---

# Transform 插件体系

## 为什么需要这篇文档

SeaTunnel 现在已经有了 Transform 插件目录页，也有 Transform 通用参数页，但还缺一篇从系统视角解释 Transform 位于整条链路哪里、共享哪些契约、贡献者该如何理解这一层的文档。

这篇文档补的就是这部分。

## Transform 位于作业链路的哪里

Transform 位于 Source 和 Sink 之间，作用对象是 SeaTunnel 自己的行模型与表模型：

```text
Source -> Transform Chain -> Sink
```

在实际作业里，Transform 块是可选的，但以下场景通常都会依赖它：

- source 字段与 sink 字段不能直接对齐
- 需要对行数据做过滤、增强或重排
- 需要把 CDC 元数据转换成下游更容易消费的形式
- 一条作业里需要路由、合并或改写多个逻辑表

SeaTunnel 通过 `plugin_output` 注册中间数据集，通过 `plugin_input` 消费一个或多个上游数据集。因此 transform 链路可以表达成逻辑图，而不只是死板的单链路。

## Transform 层承担什么职责

从系统角度看，Transform 不只是字段映射，它主要承担以下职责：

- 在不绑定某个引擎原生 record 的前提下改写行数据
- 在字段新增、删除、重命名时保留或更新 schema 信息
- 把 row kind、event time 等元数据暴露成普通字段，便于下游使用
- 在多表作业中做路由、合并、过滤等逻辑编排
- 让作业逻辑保持声明式，从而可在不同执行引擎之间复用

这也是为什么 Transform 层在批处理和 CDC 链路里都非常重要。

## 核心契约

Transform 体系主要围绕下面这些契约构建：

- `SeaTunnelTransform`：基础运行时契约
- `SeaTunnelMapTransform`：一进一出的行转换
- `SeaTunnelFlatMapTransform`：一进零到多出的行转换
- `TableTransform`：用于创建运行时 transform 实例的包装层
- `TableTransformFactory`：基于 SPI 的工厂入口
- `TableTransformFactoryContext`：向工厂传递 `ReadonlyConfig`、类加载器和上游 `CatalogTable` 元数据的上下文

之所以这样拆，是因为 SeaTunnel 希望 Transform 插件同时满足：

- 对用户来说是声明式的
- 对贡献者来说是引擎无关的
- 对规划器来说是可感知元数据的

相关文档：

- [核心 API 设计](./core-api-design.md)
- [配置与 Option 系统](./configuration-and-option-system.md)
- [插件发现与类加载](./plugin-discovery-and-class-loading.md)

## Transform 如何被准备和执行

从高层看，Transform 的准备流程大致如下：

1. 作业配置定义 transform 块和对应参数
2. SeaTunnel 通过 factory 与 SPI 机制发现匹配的 `TableTransformFactory`
3. 在真正创建运行时 transform 之前先校验配置
4. 把上游 `CatalogTable` 元数据放入 transform factory context
5. 把运行时 transform 插入逻辑 pipeline，随后再适配到具体执行引擎

关键设计点在于：Transform 插件首先作用于 SeaTunnel 自己的契约，Flink、Spark 或 Zeta 的适配发生在后面。

## 常见 Transform 类型

当前 Transform 生态已经比较丰富，但大致可以归为几类：

### 行投影与字段映射

- [FieldMapper](../transforms/field-mapper.md)
- [FieldRename](../transforms/field-rename.md)
- [Copy](../transforms/copy.md)

这类插件主要用于把上游字段整理成下游期望的 schema。

### 过滤与路由

- [Filter](../transforms/filter.md)
- [TableFilter](../transforms/table-filter.md)
- [TableMerge](../transforms/table-merge.md)

这类插件负责决定哪些记录或哪些逻辑表继续沿链路流动。

### SQL 与表达式类处理

- [SQL](../transforms/sql.md)
- [JsonPath](../transforms/jsonpath.md)
- [RegexExtract](../transforms/regexextract.md)

当转换逻辑更适合用声明式方式表达，而不是自定义代码时，这类插件会更合适。

### 元数据与 CDC 适配

- [Metadata](../transforms/metadata.md)
- [RowKindExtractor](../transforms/rowkind-extractor.md)
- [FilterRowKind](../transforms/filter-rowkind.md)

这类插件在 CDC 链路里尤为关键，因为它们能把变化语义保留下来，或者改造成下游更容易消费的形态。

### 可编程或 AI 相关处理

- [DynamicCompile](../transforms/dynamic-compile.md)
- [LLM](../transforms/llm.md)
- [Embedding](../transforms/embedding.md)

这类插件适用于需要更复杂业务逻辑、外部模型或可编程处理能力的场景。

## 给贡献者的设计建议

新增或评审 Transform 插件时，建议先检查这些点：

- 保持 Transform 契约对执行引擎无感
- 用稳定的 `Option` 与 `OptionRule` 定义用户可见参数
- 对 schema 变化给出明确行为，而不是把歧义留给下游
- 如果插件支持多表模式，要明确处理多输入与多输出
- 不要把 source 专属或 sink 专属职责塞进 transform 层

一般来说，Transform 层应该负责行数据与 schema 的改写，而不是外部提交语义或引擎运行时细节。

## 常见误解

### “Transform 只是可有可无的修饰层”

并不是。很多作业里真正的业务映射、schema 对齐和 CDC 适配都发生在 Transform 层。

### “Transform 只处理行数据，不处理 schema”

也不对。特别是在多表和 CDC 场景下，很多 transform 同时需要保留或改写 schema 与元数据。

### “能在一个引擎上跑通，就天然具备可移植性”

可移植性是设计目标，不是自然副作用。贡献者仍然需要避免引擎特定假设，并遵守 SeaTunnel 的 API 契约。

## 推荐阅读顺序

1. 先读本页，建立整体视角
2. 再读 [Transform 通用参数](../transforms/common-options/common-options.md)
3. 再读 [核心 API 设计](./core-api-design.md)
4. 再读 [CDC Pipeline 架构概览](./cdc-pipeline-architecture.md)
5. 再读 [插件发现与类加载](./plugin-discovery-and-class-loading.md)
6. 最后按需回到 [Transforms 目录](../transforms)
