---
sidebar_position: 2
---

# 工作原理

## 新用户先抓住这几点

在第一次使用 SeaTunnel 时，您不需要先理解所有内部模块。
对大多数新用户来说，更实用的顺序是：

1. 先在本地跑通一个任务
2. 再理解配置文件结构
3. 然后选择合适的连接器和执行引擎
4. 当您需要理解运行模型时，再回到这一页

把 SeaTunnel 先理解成“一条由配置驱动、运行在某个执行引擎上的数据管道”，通常最容易入门。

## 概述

SeaTunnel 是一个分布式多模态数据集成工具，采用插件化架构。连接器层与执行引擎解耦，同一套连接器可在不同引擎上运行。

这一页适合作为“快速开始”和“架构章节”之间的桥接页。当你已经知道 SeaTunnel 是什么，但还没形成“作业配置、插件体系、执行引擎如何连起来”的整体模型时，建议先读这里。

```mermaid
flowchart TD
    config["作业配置<br/>HOCON / SQL / Web UI"]
    core["SeaTunnel 核心层<br/>作业解析器 / 协调器 / 调度器"]
    source["Source 数据源连接器"]
    transform["Transform（可选）"]
    sink["Sink 目标连接器"]
    engine["执行引擎<br/>SeaTunnel Engine (Zeta) / Flink / Spark"]

    config --> core
    core --> source
    source --> transform
    transform --> sink
    sink --> engine

    classDef layerBlue fill:#0f1d33,stroke:#5db8e2,stroke-width:2px,color:#f8fbff;
    classDef layerCyan fill:#0c2530,stroke:#2dd4bf,stroke-width:2px,color:#f8fbff;
    classDef layerPurple fill:#1f1a34,stroke:#8d7cf6,stroke-width:2px,color:#f8fbff;

    class config,core layerBlue;
    class source,transform,sink layerCyan;
    class engine layerPurple;
    linkStyle default stroke:#5db8e2,stroke-width:2px;
```

## 四个核心构件

### 1. 作业配置

配置文件描述了读什么、怎么转换、写到哪里，以及需要使用哪些引擎参数。

### 2. SeaTunnel 核心层

SeaTunnel 会解析配置、生成执行计划、加载插件，并把作业提交到选定的执行引擎。

### 3. 数据链路：Source -> Transform -> Sink

这是大多数新用户最应该先记住的数据路径：

- **Source（读取）** 负责从外部系统读取数据
- **Transform（转换）** 负责按需做字段映射、过滤或简单转换
- **Sink（写入）** 负责把结果写入目标系统

### 4. 执行引擎

引擎决定作业最终跑在哪儿。对大多数新用户来说，建议先从 [SeaTunnel 引擎（Zeta）](../engines/zeta/about.md) 开始；只有在现有环境已经依赖 Flink 或 Spark 时，再切换到对应引擎。

## 推荐阅读路径

如果你希望先建立一套系统级理解，建议按下面顺序阅读：

- [快速入门总览](../getting-started/overview.md)，先拿到最短首跑路径
- 本页，先建立执行模型的整体图景
- [引擎概览](../engines/overview.md)，理解执行引擎如何选择
- [架构概览](../architecture/overview.md)，再进入更完整的分层视图
- [核心 API 设计](../architecture/core-api-design.md)，理解连接器与元数据契约
- 如果你还需要理解数据集编排与 transform 行为，再看 [Transform 插件体系](../architecture/transform-plugin-system.md)

## 核心组件

### 1. 连接器接口（Connector API）

与引擎无关的统一接口，用于开发 Source、Transform、Sink 连接器。

| 组件 | 说明 |
|------|------|
| **Source（读取）** | 从外部系统读取数据（数据库、文件、消息队列） |
| **Transform（转换）** | 数据转换（字段映射、过滤、类型转换） |
| **Sink（写入）** | 将数据写入目标系统 |

### 2. 执行引擎

| 引擎 | 适用场景 |
|------|---------|
| **SeaTunnel 引擎（Zeta）** | 数据同步、CDC、低资源消耗 |
| **Apache Flink** | 复杂流处理、已有 Flink 基础设施 |
| **Apache Spark** | 大规模批处理、已有 Spark 基础设施 |

### 3. 翻译层

将 SeaTunnel 统一 API 转换为引擎特定实现，实现连接器跨引擎复用。

## 数据流

```mermaid
flowchart LR
    source["Source"] --> split["分片"] --> reader["Reader"] --> transform["Transform"] --> writer["Writer"] --> sink["Sink"]
    reader -. "Checkpoint / 状态" .-> recovery["容错机制"]
    writer -. "Checkpoint / 提交" .-> recovery
    source -. "重放 / 重读" .-> recovery

    classDef layerBlue fill:#0f1d33,stroke:#5db8e2,stroke-width:2px,color:#f8fbff;
    classDef layerCyan fill:#0c2530,stroke:#2dd4bf,stroke-width:2px,color:#f8fbff;

    class source,split,reader,transform,writer,sink layerBlue;
    class recovery layerCyan;
    linkStyle default stroke:#5db8e2,stroke-width:2px;
```

**核心特性：**
- 基于分片的并行读取
- 分布式快照实现精确一次语义
- 自动故障转移和恢复

## 模块结构

| 模块 | 职责 |
|------|------|
| `seatunnel-api` | 核心 API 定义 |
| `seatunnel-connectors-v2` | Source 和 Sink 连接器 |
| `seatunnel-transforms-v2` | Transform 插件 |
| `seatunnel-engine` | SeaTunnel 引擎（Zeta） |
| `seatunnel-translation` | Flink 和 Spark 的引擎适配器 |
| `seatunnel-core` | 作业提交与 CLI |
| `seatunnel-formats` | 数据格式处理 |
| `seatunnel-e2e` | 端到端测试 |

## 作业执行流程

1. **解析** - 读取并验证作业配置
2. **规划** - 生成带并行度的执行计划
3. **调度** - 将任务分发到 Worker 节点
4. **执行** - 运行 Source → Transform → Sink 管道
5. **监控** - 跟踪进度、指标和检查点

## 下一步

- [引擎对比](../engines/overview.md)
- [快速入门总览](../getting-started/overview.md)
- [SeaTunnel 引擎快速开始](../getting-started/locally/quick-start-seatunnel-engine.md)
- [架构概览](../architecture/overview.md)
- [数据连接器总览](../connectors)
