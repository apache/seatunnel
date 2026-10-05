---
slug: /getting-started/locally
---

# 本地快速开始

如果你的目标是用最短路径把 SeaTunnel 在本机跑起来，就先看这一页。对大多数第一次接触 SeaTunnel 的用户来说，默认最推荐的仍然是 **SeaTunnel 引擎（Zeta）**，因为它部署最短、反馈最快、最适合作为首跑路径。

## 先选一条本地起步路径

| 你的情况 | 推荐入口 |
| --- | --- |
| 我想走默认的首跑路径 | [SeaTunnel 引擎快速开始](./quick-start-seatunnel-engine.md) |
| 我需要先完成安装和插件准备 | [部署](./deployment.md) |
| 团队已经有 Flink 环境 | [Flink 引擎快速开始](./quick-start-flink.md) |
| 团队已经有 Spark 环境 | [Spark 引擎快速开始](./quick-start-spark.md) |

## 推荐首跑顺序

1. 先看 [部署](./deployment.md)。
2. 安装示例任务所需插件。
3. 通过 [跑第一个任务](./run-your-first-job.md) 或 [SeaTunnel 引擎快速开始](./quick-start-seatunnel-engine.md) 跑通首个本地作业。
4. 示例成功后，再进入 [作业配置指南](../job-configuration-guide.md) 编写真实作业。

## 什么时候再看其他路径

- 只有在你已经维护 Flink 集群时，才优先走 Flink 路径。
- 只有在你的现有作业体系本来就围绕 Spark 时，才优先走 Spark 路径。
- 更稳妥的顺序仍然是先把本地链路跑通，再进入集群部署或远程提交。
