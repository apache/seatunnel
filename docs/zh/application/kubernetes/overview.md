---
sidebar_position: 1
title: Kubernetes 概览
---

# Kubernetes Application Mode

Kubernetes Application Mode 为一个 SeaTunnel 作业创建一个独立的 Kubernetes application。一个 `batch/v1` Job 承载唯一的 Zeta Master，Master 再申请固定数量的 Worker Pod。所有进程使用同一 SeaTunnel 镜像，作业结束后清理 Worker 和 application 资源。

它适合已经使用 Kubernetes 管理计算资源，并希望按作业隔离资源、依赖和故障的场景。执行过程使用原生 Zeta Engine，不依赖 Flink 或 Spark。

## 运行形态

| 组件 | Kubernetes 资源 | 职责 |
| --- | --- | --- |
| Application Master | 一个 Job Pod | 启动 Zeta Master、创建 Worker、提交作业和协调清理 |
| Worker | N 个普通 Pod | 加入独立 Hazelcast 集群并提供固定 task slot |
| 作业配置 | ConfigMap | 只挂载给 Master，包含解析后的作业与部署信息 |
| Master 发现 | Headless Service | 为 Worker 提供当前 application 的 Master 地址 |
| Checkpoint | PVC 或受支持的远程存储 | 在 application 之外保存可恢复状态 |

Master 或任意 Worker 丢失都会使当前 Job 失败。系统不会自动补建 Worker 或启动备用 Master。持久化 checkpoint 可用于创建新的 application 恢复执行。

## 前置条件

- Kubernetes 1.24 或更高版本，支持 `batch/v1` Job、挂起 Job 和 Job TTL。
- 可用的 `kubectl` 和提交端 kubeconfig，或可用的集群内身份。
- 已创建 Namespace、ServiceAccount 和最小 RBAC。
- Master 与 Worker Pod 之间允许 Hazelcast TCP 通信。
- 集群可以拉取包含 SeaTunnel、Kubernetes provider 和作业插件的镜像。
- 提交身份可以创建和管理 Job、ConfigMap、Service 与 Pod。

## 推荐阅读顺序

| 阶段 | 文档 | 内容 |
| --- | --- | --- |
| 理解部署 | [集群架构](architecture.md) | Job、Worker Pod、Service、ConfigMap 和存储关系 |
| 首次运行 | [快速开始](quick-start.md) | 构建镜像、创建 RBAC 并提交第一个作业 |
| 调整参数 | [配置参考](configuration.md) | 完整 Kubernetes 与公共 application 选项 |
| 配置容错 | [Checkpoint 恢复](checkpoint-recovery.md) | PVC、HDFS、对象存储与故障后恢复 |
| 问题排查 | [FAQ](faq.md) | 状态、取消、日志、资源清理和常见问题 |

公共能力边界参见 [Application Mode 概览](../overview.md) 和[架构概览](../architecture.md)。
