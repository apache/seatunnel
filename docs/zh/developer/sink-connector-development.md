---
title: Sink Connector 开发指南
---

# Sink Connector 开发指南

## 目标

这篇文档是面向贡献者实现 SeaTunnel sink connector 的实操入口，重点放在编码前该做哪些设计判断，以及评审时应重点看什么。

## Sink 开发为什么更难

Sink 通常比 Source 更难做好，因为它面对的是外部副作用。

一个 sink connector 必须先把自己的语义讲清楚：

- 是 append-only、at-least-once，还是更强的一致性语义
- commit 是幂等的还是依赖事务
- insert、update、delete 怎么处理
- 目标系统对 schema 的兼容边界是什么

如果这些语义没有写清楚，这个 connector 很容易在 demo 场景里看起来没问题，但一到 retry 或恢复场景就出错。

## 推荐开发流程

### 1. 先定义写入契约

在开始实现之前，先说明清楚：

- 目标系统模型是 append、overwrite、upsert，还是表级事务提交
- 是否依赖主键
- 是否支持 delete
- 如何处理 schema evolution
- 失败和重试时的行为是什么

这些内容应该同时体现在文档和代码中。

### 2. 定义稳定的 Options

Sink factory 应该清晰定义：

- required options
- optional options
- default value
- mutually exclusive 或 bundled 规则

不要把 option 名当成临时实现细节，它们是用户可见契约。

相关文档：

- [配置与 Option 系统](../architecture/configuration-and-option-system.md)
- [作业配置指南](../getting-started/job-configuration-guide.md)

### 3. 先选提交模型

SeaTunnel 的 sink 设计支持多种复杂度层级：

- 只有 writer
- writer + committer
- writer + committer + aggregated committer

应该根据目标系统特性来选，不要默认套最复杂，也不要为了省事牺牲正确性。

### 4. 实现运行时与打包

一个完整的 sink connector 贡献通常包括：

- sink factory
- `SeaTunnelSink`
- `SinkWriter`
- 可选的 `SinkCommitter`
- 可选的 `SinkAggregatedCommitter`
- 打包与发现元数据

### 5. 验证恢复行为

不要在 happy path 写入成功后就停下。还应该确认这些情况：

- `prepareCommit` 之后任务失败会怎样
- commit 被重试会怎样
- sink 收到重复 commit 请求会怎样
- 目标表 schema 变化后会怎样

## 设计检查清单

编码前，建议先回答这些问题：

- 这个 sink 是 append-only 还是 CDC-aware
- 目标系统是否支持幂等 upsert
- 是否需要透传 delete
- exactly-once 风格提交是否依赖事务
- checkpoint 恢复后要恢复哪些状态
- 同一个 commit 被重放两次会发生什么

## 典型类结构

```text
connector-<name>/
  src/main/java/.../sink/
    <Name>SinkFactory.java
    <Name>Sink.java
    <Name>SinkWriter.java
    <Name>SinkConfig.java
```

根据语义复杂度，通常还会补这些类：

- `<Name>CommitInfo`
- `<Name>WriterState`
- `<Name>SinkCommitter`
- `<Name>SinkAggregatedCommitter`
- schema 或 table 辅助类

## 提交模型选择

### 只有 Writer

适用于：

- at-least-once 或更弱语义可以接受
- 目标系统天然支持幂等
- 不需要集中式 commit 协调

### Writer + Committer

适用于：

- 每个 writer 独立 prepare 自己的结果
- commit 可以按 writer 或 partition 分别进行
- 需要显式集中处理 retry

### Writer + Aggregated Committer

适用于：

- sink 需要单一的表级或全局 commit 点
- 所有 writer 的输出必须先汇总再整体可见
- 故障处理需要全局协调

对表类 sink 和强一致性场景，这种模型尤其重要。

## CDC-Aware Sink 设计

如果 sink 接受 CDC 输入，必须把映射规则写清楚：

- insert -> ?
- update -> ?
- delete -> ?

同时还要明确：

- 是否要求主键
- schema change 是否自动应用
- 对不支持的 row kind，是拒绝、忽略，还是要求上游先做转换

相关文档：

- [CDC Pipeline 架构概览](../architecture/cdc-pipeline-architecture.md)
- [Sink 架构](../architecture/api-design/sink-architecture.md)

## 常见坑点

### 在 `prepareCommit` 里直接做最终提交

除非你明确设计的是更简单的语义并已写进文档，否则 `prepareCommit` 不应该偷偷变成真正的最终 commit 点。

### Retry 不是幂等的

如果 commit 在失败后会再次执行，重复副作用不能把目标系统写坏。

### 隐藏语义边界

如果 sink 不支持 delete、schema evolution 或 exactly-once 风格恢复，应该在文档里明确写出来，而不是让用户靠试错发现。

## 测试策略

至少建议覆盖：

- option 校验
- writer 行为
- prepare commit 行为
- retry 与幂等行为
- checkpoint 或重启后的恢复

如果这个 sink 是生产常用 connector，强烈建议补 E2E 覆盖。

## 打包检查清单

提交 PR 前建议确认：

- factory 注册已存在
- connector 已加入打包
- 需要时已更新 plugin mapping 与依赖目录布局
- 文档示例与真实 plugin identifier 一致
- 中英文文档都已更新

## 推荐阅读顺序

1. 先读本页，建立设计检查表
2. 再读 [Sink 架构](../architecture/api-design/sink-architecture.md)
3. 再读 [Exactly-Once](../architecture/fault-tolerance/exactly-once.md)
4. 再读 [插件发现与类加载](../architecture/plugin-discovery-and-class-loading.md)
5. 最后结合 [开发自己的 Connector](./how-to-create-your-connector.md)
