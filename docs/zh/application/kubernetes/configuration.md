---
sidebar_position: 4
title: 配置参考
---

# Kubernetes 配置参考

## Kubernetes 选项

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `kubernetes.namespace` | `default` | 所有 application 资源所在的已有 Namespace。 |
| `kubernetes.image` | 必填 | 包含 SeaTunnel、provider 和作业插件的镜像。 |
| `kubernetes.image-pull-policy` | `IfNotPresent` | `Always`、`IfNotPresent` 或 `Never`。 |
| `kubernetes.service-account` | `default` | Master 使用的已有 ServiceAccount。 |
| `kubernetes.seatunnel-home` | `/opt/seatunnel` | 镜像内发行包绝对路径，不能包含 `:`。 |
| `kubernetes.kubeconfig` | SDK 默认 | 提交端本地 kubeconfig；不会分发到 Master。 |
| `kubernetes.finished-job-retention-seconds` | `86400` | 完成或失败 Job 的保留秒数，必须为正数。 |
| `kubernetes.checkpoint-pvc` | 未设置 | 已有 PVC，挂载到 Master 的 `/opt/seatunnel/checkpoints`。 |

## Application 公共选项

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `application.name` | `seatunnel` | 唯一 Job 名称的可读前缀。 |
| `application.job-id` | 自动生成 | 正数原生 Zeta job ID。 |
| `application.restore-job-id` | 未设置 | 从历史 job ID 的最新有效 checkpoint 恢复。 |
| `application.worker-count` | `1` | 固定 Worker Pod 数量。 |
| `application.worker.memory-mb` | `1024` | 每个 Worker 的 memory request 和 limit，单位 MiB。 |
| `application.worker.cpu-cores` | `1` | 每个 Worker 的 CPU request 和 limit。 |
| `application.worker.slots` | `2` | 每个 Worker 的固定执行 slot。 |
| `application.master.memory-mb` | `1024` | Master memory request 和 limit，单位 MiB。 |
| `application.master.cpu-cores` | `1` | Master CPU request 和 limit。 |
| `application.master.port` | `5801` | Master Hazelcast TCP 端口。 |
| `application.startup-timeout-millis` | `120000` | 分别用于等待 Master 启动，以及 Worker 创建和注册。 |

每个 Pod 的 JVM 堆使用内存上限的 75%，剩余空间用于堆外内存和 JVM 开销。

`application.startup-timeout-millis` 不是作业执行时长限制。镜像拉取、准入或调度问题会使提交端等待 Master 超时；Master 使用同一超时等待 Worker。

## 镜像和凭据

Provider 不提供任意 Pod template 或 image-pull-secret 配置。私有镜像凭据应绑定到 ServiceAccount 或由集群管理。Connector 引用的本地文件和 JAR 不会动态下载，必须已经存在于镜像中。

作业配置会存入 Namespace 内的 ConfigMap，只挂载给 Master。配置可能包含 Connector 凭据，应限制 ConfigMap 读取权限。敏感信息应使用底层文件系统、Connector 或集群支持的凭据机制。

## 容量规划

Pod request 与 limit 相同。需要同时规划 Worker 数、slot 数、作业并行度、Namespace ResourceQuota、LimitRange 和节点容量。Master 不提供 task slot。
