---
title: Flink Translation Layer
---

# Flink 转换层

## 目的

这篇文档解释 SeaTunnel 是如何把引擎无关的 connector API 适配到 Apache Flink 的。

如果你只需要整体视图，建议先读 [转换层](./translation-layer.md)。本页聚焦的是 Flink 这条具体适配路径。

## 为什么 Flink 需要转换层

SeaTunnel connector 作者实现的是 SeaTunnel API，例如：

- `SeaTunnelSource`
- `SeaTunnelSink`
- `SeaTunnelTransform`

但 Flink 运行作业依赖的是它自己的 source / sink 运行时、checkpoint 生命周期和上下文接口。

Flink 转换层存在的意义是：

- connector 作者不需要直接写 Flink 专属 connector 代码
- Flink 作业仍然能保持 SeaTunnel 的语义约束
- Flink API 的变化尽量被隔离在转换层内部

## 高层映射关系

从概念上看，这条映射链路大致如下：

```text
SeaTunnelSource -> FlinkSource adapter -> Flink Source runtime
SeaTunnelSink   -> FlinkSink adapter   -> Flink Sink runtime
SeaTunnel types -> serializer and type adapters -> Flink state and records
```

转换层主要适配四件事：

- 生命周期
- 上下文
- 序列化
- checkpoint 语义

## Source 侧映射

在 source 侧，Flink adapter 要把 SeaTunnel 的 reader / enumerator 模型桥接到 Flink source runtime。

典型职责包括：

- 把 SeaTunnel boundedness 映射成 Flink boundedness
- 从 SeaTunnel reader 创建 Flink `SourceReader` 适配器
- 从 SeaTunnel split enumerator 创建 Flink enumerator 适配器
- 把 split 与 enumerator state 的 serializer 包装成 Flink 可 checkpoint 的形式

这条路径之所以适配性较好，是因为：

- SeaTunnel 和 Flink 都把 source 的协调端与执行端分离了
- split-based source 设计与 Flink 运行模型天然接近

相关文档：

- [Source 架构](./source-architecture.md)

## Sink 侧映射

在 sink 侧，Flink 转换层把 SeaTunnel sink 契约映射到 Flink 的 writer / committer 模型。

典型职责包括：

- 从 `SeaTunnelSink` 创建 Flink writer
- 通过 Flink 兼容的提交流程暴露 SeaTunnel committer 和 aggregated committer 语义
- 映射 writer state 与 commit info 的 serializer

这在 sink 使用 checkpoint 驱动提交语义时尤其关键。

相关文档：

- [Sink 架构](./sink-architecture.md)
- [Exactly-Once](../fault-tolerance/exactly-once.md)

## Checkpoint 与状态对齐

Flink 是 SeaTunnel 当前 API 设计的重要参照之一。

Flink 转换层必须保证下面这些语义对齐：

- state snapshot 的时机
- checkpoint complete 回调
- split / writer state 的序列化
- commit 协调语义

如果这层对齐出错，用户通常看到的现象会是：

- 数据重复
- 恢复后数据缺失
- checkpoint 失败
- sink commit 不一致

## 上下文适配

Flink runtime context 暴露出来的 API 和 SeaTunnel 接口并不是一一对应的，因此转换层需要包装：

- source reader context
- split enumerator context
- sink writer context
- event 与 metrics 通道

这部分虽然不显眼，但非常关键，因为它阻止了 connector 实现直接依赖 Flink 内部细节。

## Serializer 适配

Flink 对 state、split、commit info 有自己的 serializer 契约。SeaTunnel 也有自己的 serializer，因此转换层需要把 SeaTunnel serializer 包装成 Flink 可接受的接口。

这直接影响：

- checkpoint 持久化
- 版本化 state 兼容性
- split 回收与恢复

## Flink 路径的优势

Flink 这条适配路径对 SeaTunnel 来说比较自然，原因包括：

- split-based source 设计适配性好
- checkpoint 语义成熟
- 有状态 source / sink 模式已经比较稳定

这也是为什么 SeaTunnel 能在 Flink 上支持复杂 connector 语义，同时不要求 connector 作者直接实现 Flink 版本代码。

## 常见问题集中区

当 Flink 转换层出现问题时，通常会集中在这些地方：

- checkpoint 回调
- serializer 兼容性
- watermark / event-time 预期
- 引擎特定配置泄漏进 connector 实现

排查时要先区分清楚，这到底是：

- connector 自身的 bug
- SeaTunnel API 契约问题
- 还是 Flink 转换层问题

## 代码入口

如果要直接看实现，建议从这些目录开始：

- `seatunnel-translation/seatunnel-translation-flink/`
- `seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/`
- `seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/src/main/java/org/apache/seatunnel/translation/flink/source/`

推荐优先看的类包括：

- `FlinkSource`
- `FlinkSourceReader`
- `FlinkSourceEnumerator`
- `FlinkSourceReaderContext`
- `FlinkSourceSplitEnumeratorContext`

## 推荐阅读顺序

1. [转换层](./translation-layer.md)
2. 本页
3. [Source 架构](./source-architecture.md)
4. [Sink 架构](./sink-architecture.md)
5. [Exactly-Once](../fault-tolerance/exactly-once.md)
