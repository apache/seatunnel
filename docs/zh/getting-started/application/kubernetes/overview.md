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
| 作业配置 | 默认内联在 Master 环境；可选挂载 `job.conf` | 由 `kubernetes.job-config-file` 选择 ConfigMap 或 Secret 文件；不设置时使用提交的 `-c` 文件 |
| 运行时配置 | 已有或自建 ConfigMap | 以只读方式挂载给 Master 和 Worker，包含 `seatunnel.yaml`、Hazelcast、JVM 参数与日志配置 |
| Master 发现 | Master Pod IP | Worker 通过该地址经 TCP 加入集群；Headless Service 只负责选中 Master Pod |
| Checkpoint | PVC 或受支持的远程存储 | 在 application 之外保存可恢复状态 |

:::caution
Master 或任意 Worker 丢失都会使当前 Job 失败。系统不会自动补建 Worker 或启动备用 Master。
持久化 checkpoint 可用于创建新的 application 恢复执行。
:::

## 能力清单

| 领域 | 本版本支持 |
| --- | --- |
| 提交与观测 | `submit`、`status`、`cancel`；提交在 Job 启动后即返回，`--wait` 可选择等待 |
| 拓扑 | 一个 Master 加固定数量的 Worker，各自独立配置 CPU、内存和 slot；一个 application 只跑一个原生作业 |
| 作业配置 | 提交的 `-c` 文件，或由 Secret/ConfigMap 挂载的文件；`${VAR}` 占位符在 Pod 内解析 |
| Secret 与 ConfigMap | 可引用已有对象（SeaTunnel 永不创建或删除），也可由 SeaTunnel 创建并持有；每个挂载 key 都声明完整路径并使用 `subPath` |
| 环境变量 | 逐变量的 `kubernetes.env.*` 与整对象的 `envFrom`，取值可来自字面量、Secret 或 ConfigMap |
| 运行时文件 | 引擎、Hazelcast、JVM 参数、Log4j2 文件均可指定，也可禁用 |
| 调度 | Namespace、ServiceAccount、镜像拉取 Secret、labels、annotations、node selector、tolerations |
| 存储与恢复 | 挂载 checkpoint PVC，并通过 `application.restore-job-id` 恢复之前的作业 |
| 生命周期 | Worker 丢失或 Master 退出都会使 application 失败；`cancel` 删除全部自有资源、保留引用资源；完成的 Job 保留 `kubernetes.finished-job-retention-seconds` |

:::note
本版本不支持：Master 高可用切换、Worker 自动补建或弹性伸缩、任意 Pod template、
affinity/PriorityClass、Kerberos、REST/HTTPS。失败的 application 可用 `application.restore-job-id`
从最近的持久化 checkpoint 恢复。
:::

## 前置条件

- Kubernetes 1.24 或更高版本，支持 `batch/v1` Job、挂起 Job 和 Job TTL。
- 可用的 `kubectl` 和提交端 kubeconfig，或可用的集群内身份。
- 已创建 Namespace、ServiceAccount 和最小 RBAC。
- 已创建包含 SeaTunnel 运行时配置的 ConfigMap，或使用 `kubernetes.configmap.<entry>.data` 让
  SeaTunnel 创建。
- Master 与 Worker Pod 之间允许 Hazelcast TCP 通信。
- 集群可以拉取包含 SeaTunnel、Kubernetes provider 和作业插件的镜像。
- 提交身份可以创建和管理 Job、Service，以及部署中自建的 Secret/ConfigMap 条目；Worker Pod
  由 application 的 ServiceAccount 创建。

## 推荐阅读顺序

| 阶段 | 文档 | 内容 |
| --- | --- | --- |
| 理解部署 | [集群架构](architecture.md) | Job、Worker Pod、Service、Secret、ConfigMap 和存储关系 |
| 首次运行 | [快速开始](quick-start.md) | 构建镜像、创建 RBAC 并提交第一个作业 |
| 运维 application | [命令参考](commands.md) | 提交、查看、等待与取消 application |
| 调整参数 | [配置参考](configuration.md) | 完整 Kubernetes 与公共 application 选项 |
| 配置容错 | [Checkpoint 恢复](checkpoint-recovery.md) | PVC、HDFS、对象存储与故障后恢复 |
| 问题排查 | [FAQ](faq.md) | 状态、取消、日志、资源清理和常见问题 |

公共能力边界参见 [Application Mode 概览](../overview.md)。
