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
| `kubernetes.image-pull-secrets` | 空 | 用于拉取私有仓库镜像的已有 Secret 名称，多个名称以逗号分隔。 |
| `kubernetes.service-account` | `default` | Master 使用的已有 ServiceAccount。 |
| `kubernetes.seatunnel-home` | `/opt/seatunnel` | 镜像内发行包绝对路径，不能包含 `:`。 |
| `kubernetes.config-map` | 未设置 | 已有 ConfigMap，以只读方式挂载到 Master 和 Worker Pod 的 `<seatunnel-home>/config`。 |
| `kubernetes.kubeconfig` | SDK 默认 | 提交端本地 kubeconfig；不会分发到 Master。应写入权限受限的部署配置文件，不要通过 `-D` 传递。 |
| `kubernetes.finished-job-retention-seconds` | `86400` | 完成或失败 Job 的保留秒数，必须为正数。 |
| `kubernetes.checkpoint-pvc` | 未设置 | 已有 PVC，挂载到 Master 的 `/opt/seatunnel/checkpoints`。 |
| `kubernetes.master.labels` | 空 | Master Pod 的附加 label，格式为逗号分隔的 `key:value`；SeaTunnel 所有权 label 为保留项。 |
| `kubernetes.worker.labels` | 空 | Worker Pod 的附加 label，格式为逗号分隔的 `key:value`；SeaTunnel 所有权 label 为保留项。 |
| `kubernetes.master.annotations` | 空 | Master Pod annotation，格式为逗号分隔的 `key:value`。 |
| `kubernetes.worker.annotations` | 空 | Worker Pod annotation，格式为逗号分隔的 `key:value`。 |
| `kubernetes.master.node-selector` | 空 | Master Pod 的 node selector，格式为逗号分隔的 `key:value`。 |
| `kubernetes.worker.node-selector` | 空 | Worker Pod 的 node selector，格式为逗号分隔的 `key:value`。 |

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

通过 `kubernetes.image-pull-secrets` 引用已有的私有镜像仓库 Secret。Provider 不创建这类凭据，也不提供任意 Pod template。Connector 引用的本地文件和 JAR 不会动态下载，必须已经存在于镜像中。

通过 `kubernetes.config-map` 指定包含 SeaTunnel 运行时配置的已有 ConfigMap，其中应包含 `seatunnel.yaml`、`log4j2_client.properties` 等配置文件。提交端会在创建 application Job 前确认该 ConfigMap 已存在；Kubernetes 随后将其以只读方式挂载到 Master 和 Worker Pod 的 `<seatunnel-home>/config`。该 ConfigMap 由用户管理，application 结束时 SeaTunnel 不会删除它。

解析后的作业配置会存入 Namespace 内的 Kubernetes Secret，并以只读方式仅挂载给 Master。Secret 可能包含 Connector 凭据，应限制 Secret 的 `get` 和 `list` 权限，并按集群安全要求启用 Kubernetes 静态加密；Secret 的 base64 表示本身不等于加密。优先使用 Connector、文件系统或集群支持的凭据机制。

命令行参数可能被提交主机上的其他用户看到，因此 `-Dkey=value` 只用于非敏感覆盖。将 `kubernetes.kubeconfig` 和其他敏感部署值写入权限受限的部署配置文件，或使用 Kubernetes SDK 默认凭据。

## 容量规划

Pod request 与 limit 相同。需要同时规划 Worker 数、slot 数、作业并行度、Namespace ResourceQuota、LimitRange 和节点容量。Master 不提供 task slot。
