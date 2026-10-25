---
title: Source Connector 开发指南
---

# Source Connector 开发指南

## 目标

这篇文档是面向贡献者的 Source Connector 实操入口。它不是为了替代底层 API 设计文档，而是帮助你把那些 API 设计真正转成一个可落地的实现计划。

如果你准备开发一个新的 source connector，建议先读本页，再进入后面的架构深挖页。

## 一个 Source Connector 必须解决什么问题

一个 source connector 至少要解决四件事：

- 定义并校验用户可见的配置参数
- 描述输出 schema
- 支持 batch、streaming 或两者兼有的数据读取
- 在需要并行时支持 split 分配与状态恢复

在 SeaTunnel 中，这通常意味着要实现：

- source factory
- `SeaTunnelSource`
- 一个或多个 `SourceReader`
- 如果要并行，还需要 split 和 enumerator

## 推荐开发流程

### 1. 先定义用户契约

在写运行时代码之前，先把这些东西定义清楚：

- plugin 名称
- required options
- optional options
- default value
- 最小可运行配置

如果你还说不清这个 connector 最小配置长什么样，通常实现也还没有真正想清楚。

相关文档：

- [作业配置指南](../getting-started/job-configuration-guide.md)
- [配置与 Option 系统](../architecture/configuration-and-option-system.md)

### 2. 实现 Factory

Factory 是用户视角下的入口，它至少要负责：

- 暴露稳定的 identifier
- 定义 `OptionRule`
- 创建 source 实例

在实际系统中，factory 也是文档、运行时校验、REST 元数据暴露、UI 配置生成之间的桥梁。

### 3. 实现 Source 运行时

简单 source 可能只需要 reader；需要扩展性和容错的 source，一般还需要 split 和 enumerator。

典型职责如下：

- `SeaTunnelSource`：顶层 source 定义
- `SourceSplitEnumerator`：发现并分配工作
- `SourceReader`：在 worker 侧真正读取数据
- serializer：在网络传输和 checkpoint 时持久化 split / enumerator 状态

### 4. 补齐打包与发现元数据

一个 connector 不是“代码能编译就完成了”。你还需要补齐：

- SPI 注册
- plugin mapping
- 分发包打包配置，让 connector jar 真正进入二进制包
- 如果需要依赖隔离，还要补齐 plugin 目录布局

### 5. 写文档和测试

一个用户可见的 connector，如果没有完成下面这些，通常不能算真的完成：

- 同步更新 `docs/en` 和 `docs/zh`
- 示例配置与代码完全一致
- 单测或 E2E 覆盖主读取路径

## 设计检查清单

编码前，先把这些问题回答清楚：

- 这个 source 是 bounded、unbounded，还是两者都支持
- split 的单位是什么：文件、分片、分区、表范围，还是别的
- reader 在没有工作时怎么继续请求任务
- 恢复时需要保存哪些状态
- schema 是自动发现还是用户配置
- 输出是单表还是多表
- 输出的是 CDC 语义还是 append-only 数据

这些答案应该驱动你的类结构，而不是反过来。

## 典型类结构

对于一个支持并行的 source，最常见的最小结构一般如下：

```text
connector-<name>/
  src/main/java/.../source/
    <Name>SourceFactory.java
    <Name>Source.java
    <Name>SourceReader.java
    <Name>SourceSplit.java
    <Name>SourceSplitEnumerator.java
    <Name>SourceConfig.java
```

复杂一点的实现通常还会加入：

- dialect 或 client 抽象
- split serializer
- enumerator state
- reader state 辅助类
- schema discoverer

## 什么时候用哪种设计

### 什么时候简单 Reader 就够了

适用于：

- 数据源天然单线程
- 不需要并行
- 没有明确的 split 模型

### 什么时候必须引入 Split 和 Enumerator

适用于：

- 数据源可以按分区或范围并行读取
- 故障后需要回收并重新分配未完成任务
- 初始发现逻辑与 worker 侧读取逻辑应当分离

对数据库、文件、队列、CDC 这类可扩展 source 来说，这基本是默认模式。

## 常见 Source 模式

### 文件 / 对象存储 Source

常见 split 单位：

- 文件
- 文件块范围
- 分区目录

常见关注点：

- 文件发现
- schema 推断
- checkpoint 当前文件位置

### 数据库快照 Source

常见 split 单位：

- 主键范围
- 分区
- shard

常见关注点：

- chunk 大小
- query pushdown
- 一致性边界

### 消息队列 Source

常见 split 单位：

- topic partition
- subscription shard

常见关注点：

- offset 管理
- watermark 或 event time
- 动态分区发现

### CDC Source

常见 split 单位：

- snapshot chunk
- incremental log split

常见关注点：

- snapshot 到 incremental 的切换
- source metadata
- schema evolution

相关文档：

- [CDC Pipeline 架构概览](../architecture/cdc-pipeline-architecture.md)

## 测试策略

至少建议覆盖这些层次：

- option 校验
- split 生成或发现逻辑
- reader 在正常数据上的行为
- checkpoint 或 state snapshot 行为
- 如果是并行 source，还要覆盖恢复或 split 回收分配

如果 connector 依赖外部系统，尽可能补或扩展 E2E 测试。

## 打包检查清单

提交 PR 前，建议确认：

- factory 注册已经存在
- connector module 已加入构建与分发
- 需要时已更新 `plugin-mapping.properties`
- 文档示例里的 plugin 名与运行时 identifier 完全一致
- 中英文文档都已补齐

## 推荐阅读顺序

1. 先读本页，建立实现检查表
2. 再读 [Source 架构](../architecture/api-design/source-architecture.md)
3. 再读 [插件发现与类加载](../architecture/plugin-discovery-and-class-loading.md)
4. 参考 `seatunnel-connectors-v2/` 下一个现有 connector
5. 最后结合 [开发自己的 Connector](./how-to-create-your-connector.md)
