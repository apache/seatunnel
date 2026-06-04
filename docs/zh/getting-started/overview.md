---
sidebar_position: 1
---

# 快速入门总览

这是一页面向第一次接触 SeaTunnel 的入口页。它的目标不是替代详细文档，而是帮助你先选对执行引擎、找到正确的快速开始路径，再按顺序进入配置、连接器和架构页面。

## SeaTunnel 能帮你做什么

SeaTunnel 是一个分布式数据集成平台，用统一的 Connector 模型处理异构系统之间的数据流转。常见场景包括：

- 数据库、文件、数据仓库之间的批量同步
- CDC 与实时同步
- 多表同步或全库迁移
- 结构化、非结构化和二进制数据的多模态集成

如果你是第一次评估 SeaTunnel，建议优先从内置的 **SeaTunnel Engine (Zeta)** 开始。它的部署路径最短，也是新项目的默认推荐执行引擎。

## 如何选择执行引擎

| 引擎 | 适用场景 | 推荐入口 |
| --- | --- | --- |
| SeaTunnel Engine (Zeta) | 新项目、CDC、低资源环境、本地快速验证 | [SeaTunnel 引擎快速开始](./locally/quick-start-seatunnel-engine.md) |
| Flink | 已有 Flink 集群和运维体系 | [Flink 快速开始](./locally/quick-start-flink.md) |
| Spark | 已有 Spark 集群和运维体系 | [Spark 快速开始](./locally/quick-start-spark.md) |

如需更完整的引擎比较，请查看 [执行引擎概览](../engines/overview.md)。

## 最短首跑路径

如果你的目标是尽快确认 SeaTunnel 可以在本地跑起来，建议按下面顺序进行：

1. 阅读 [安装部署](./locally/deployment.md)，完成二进制包安装。
2. 安装示例任务所需的插件。
3. 使用 `FakeSource -> FieldMapper -> Console` 跑通本地 SeaTunnel Engine 快速开始。
4. 示例成功后，再替换成真实的 Source 和 Sink。

## 推荐阅读路径

### 路径 A：我只想先跑通第一个任务

- [安装部署](./locally/deployment.md)
- [SeaTunnel 引擎快速开始](./locally/quick-start-seatunnel-engine.md)
- [作业配置指南](./job-configuration-guide.md)

### 路径 B：我已经知道要接什么数据源

- [作业配置指南](./job-configuration-guide.md)
- [Source 连接器列表](../connectors/source)
- [Sink 连接器列表](../connectors/sink)
- [Transform 列表](../transforms)

### 路径 C：我想先理解整体架构

- [关于 SeaTunnel](../introduction/about.md)
- [工作原理](../introduction/how-it-works.md)
- [架构概览](../architecture/overview.md)

## 开始之前需要准备什么

- Java 8 或 Java 11，并正确设置 `JAVA_HOME`
- 从 [下载页面](https://seatunnel.apache.org/download) 获取 SeaTunnel 二进制包
- 在 `${SEATUNNEL_HOME}/connectors/` 下安装所需插件
- 如果所选连接器依赖第三方驱动，还需要准备对应 jar 包

如果你只是要运行示例任务，通常需要的插件是 `connector-fake` 和 `connector-console`。

## 首跑成功后下一步做什么

跑通示例之后，下一步通常会进入以下几类工作：

- 把 `FakeSource` 和 `Console` 替换成真实连接器
- 从本地验证切换到集群部署
- 开启 REST API 与 Web UI，提升运维可观测性

推荐继续阅读：

- [作业配置指南](./job-configuration-guide.md)
- [SeaTunnel Engine(Zeta) 安装部署](../engines/zeta/deployment.md)
- [REST API 与 Web UI](../engines/zeta/rest-api-and-web-ui.md)
