---
sidebar_position: 1
title: 关于 Edge Agent
---

# Edge Agent 简介

Edge Agent 是 SeaTunnel Edge Agent 的运维与部署文档入口：在仅能本机访问数据源的边缘主机上采集数据，经 WAL 缓冲后，通过 EdgeSocket 行协议发送到正在运行的 SeaTunnel 作业。

Edge Agent 不能替代 SeaTunnel Engine。典型拓扑如下：

```text
  边缘主机                         SeaTunnel Engine 集群
  +------------------+              +---------------------------+
  | Edge Agent       |  EdgeSocket  | 含 EdgeSocket Source 的作业 |
  |（本模块）          | -----------> |（接入 + 后续管道）           |
  +------------------+              +---------------------------+
```

## 适用场景

- 数据源只存在于边缘机器（例如 /var/log、应用日志目录）。
- 需要在本机持久化后再通过网络发送的小型常驻进程。
- 下游管道在 Engine 侧使用 [EdgeSocket Source](../connectors/source/EdgeSocket.md)。

## 不适用场景

- 数据源可从 Engine 集群直接访问（在 Engine 上使用 Connector Source 即可）。
- 需要在边缘主机运行完整 SeaTunnel 转换/写出编排（应在边缘部署 Engine Worker 或其他运行时）。

## 术语表

| 术语 | 定义 |
|------|------|
| WAL | Edge Agent 本地出站队列持久化机制，用于在引擎对批次返回 RECEIVED 前保存并重试出站记录。 |
| BEST_EFFORT | 当前版本的投递语义：写入本地 WAL 并在收到 RECEIVED 前重试，可能重复投递。 |
| WAL 行状态 | PENDING（待发送）、SENDING（发送中）、ACKED（引擎已对批次返回 RECEIVED）、DEAD（超过重试上限）。 |
| Engine 响应码 | ACK 仅表示认证成功；RECEIVED 表示批次已被接入侧接受，并将 SENDING 的 WAL 行推进到 ACKED。其他响应包括 AUTH_FAILED、REJECTED、RETRY、QUEUE_FULL、DECRYPT_FAILED。 |

## 推荐阅读顺序

建议按以下顺序阅读：

| 阶段 | 文档 | 说明 |
|------|------|------|
| 快速体验 | [快速开始](quick-start.md) | 本地验证 → 接入 Engine |
| 安装部署 | [下载](download.md) / [部署指南](deployment-guide.md) | 安装包获取与生产部署步骤 |
| 采集配置 | [输入配置](input-configuration.md) / [输出配置](output-configuration.md) | 按场景编写 YAML |
| 参数参考 | [配置参数说明](configuration.md) | agent.yaml 全量参数表 |
| 深入理解 | [架构概览](./architecture-overview.md) | 设计原理、可靠性与 Engine 边界 |
| Engine 侧 | [EdgeSocket Source](../connectors/source/EdgeSocket.md) | Engine 端接收协议 |
| 日常运维 | [运维](operations.md) | 启停、日志、排障 |
