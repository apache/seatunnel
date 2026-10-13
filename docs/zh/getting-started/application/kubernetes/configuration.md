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
| `kubernetes.configmap.<entry>` | 未设置 | ConfigMap 挂载条目：`ref` 引用已有 ConfigMap 并按 `subPath` 挂载声明的 key；`data` 则由 SeaTunnel 创建并持有 ConfigMap。 |
| `kubernetes.secret-ref` | 未设置 | 已有 Secret，只读挂载到 `/etc/seatunnel-secrets/application`，永不创建或删除；其中 `job.conf` 键会替换内联的作业配置。 |
| `kubernetes.secret.name` | application Job 名称 | SeaTunnel 根据 `kubernetes.secret.data.*` 创建 Secret 时使用的名称。 |
| `kubernetes.secret.data.<KEY>` | 未设置 | SeaTunnel 创建的 application Secret 键值；该 Secret 由 Job 拥有，取消时自动删除。 |
| `kubernetes.env` | 空 | 注入 Master 和 Worker 容器的环境变量条目，逗号分隔：`NAME=value`、`NAME=secret:<name>/<key>` 或 `NAME=configmap:<name>/<key>`。 |
| `kubernetes.kubeconfig` | SDK 默认 | 提交端本地 kubeconfig；不会分发到 Master。应写入权限受限的部署配置文件，不要通过 `-i` 传递。 |
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
| `application.startup-timeout-millis` | `120000` | 用于 Master 等待 Worker 创建和注册，以及客户端连接建立；提交本身不等待 Master 启动。 |

每个 Pod 的 JVM 堆使用内存上限的 75%，剩余空间用于堆外内存和 JVM 开销。

`application.startup-timeout-millis` 不是作业执行时长限制，也不限制提交。提交器在 API 接受并启动 Job 后立即返回，镜像拉取慢、准入或调度问题不会使提交命令失败，可通过 `status` 和 Pod 事件观察；Master 使用同一超时等待 Worker。

## 镜像和凭据

通过 `kubernetes.image-pull-secrets` 引用已有的私有镜像仓库 Secret。Provider 不创建这类凭据，也不提供任意 Pod template。Connector 引用的本地文件和 JAR 不会动态下载，必须已经存在于镜像中。

通过 `kubernetes.config-map` 指定包含 SeaTunnel 运行时配置的已有 ConfigMap，其中应包含 `seatunnel.yaml`、`log4j2_client.properties` 等配置文件。提交端会在创建 application Job 前确认该 ConfigMap 已存在；Kubernetes 随后将其以只读方式挂载到 Master 和 Worker Pod 的 `<seatunnel-home>/config`。该 ConfigMap 由用户管理，application 结束时 SeaTunnel 不会删除它。

默认情况下，本地化后的 application 配置（含作业配置）通过 Master 容器的环境变量传递，因此提交不需要创建 Secret 的权限。`kubernetes.secret-ref` 只读引用平台管理的已有 Secret，`kubernetes.secret.data.*` 则由 SeaTunnel 创建并持有 Secret；两种方式中若包含 `job.conf` 键，会替换内联的作业配置。引用型 Secret 不会被修改或删除，创建型 Secret 带 Job ownerReference，随 application 一起清理。请把 Connector 凭据放在平台管理的 Secret 中，不要内联到本地化配置。

提交机能解析的变量会在提交前完成替换；剩余的 `${VAR}` 占位符保留在作业配置中，由 Master 在 Pod 内使用环境变量解析，包括通过 `kubernetes.env` 的 `secretKeyRef`、`configMapKeyRef` 注入的值。任何位置都不存在的占位符会在 Master 启动时报告，而不是提交时立即失败。

### Application Secret

引用平台创建并管理的 Secret；SeaTunnel 只读挂载，永不创建或删除：

```hocon
kubernetes.secret-ref = seatunnel-job-secret
```

也可以由 SeaTunnel 创建并持有 Secret。`data` 中的条目会挂载到 `/etc/seatunnel-secrets/application`；通过 `kubernetes.env` 注入为 Pod 环境变量后，作业配置里的 `${VAR}` 占位符即可在 Master 内解析：

```hocon
kubernetes {
  secret {
    name = seatunnel-job-secret
    data {
      MYSQL_PASSWORD = ${MYSQL_PASSWORD}   # 在提交机解析
      S3_SECRET_KEY = "s3-secret-key"
    }
  }
  env = "MYSQL_PASSWORD=secret:seatunnel-job-secret/MYSQL_PASSWORD,S3_SECRET_KEY=secret:seatunnel-job-secret/S3_SECRET_KEY"
}
```

`kubernetes.secret-ref` 与 `kubernetes.secret` 互斥；Secret 的 namespace 始终跟随 `kubernetes.namespace`；包含 `job.conf` 键时会替换内联的作业配置。

### Application ConfigMap

引用已有 ConfigMap 时，用 `mount` 声明要挂载的 key 及其完整路径，按 `subPath` 单文件挂载；引用型 ConfigMap 不会被修改或删除，挂载的 key 会在提交前校验：

```hocon
kubernetes.configmap.shared {
  ref = platform-runtime
  mount {
    seatunnel.yaml = /opt/seatunnel/config/seatunnel.yaml
    log4j2_client.properties = /opt/seatunnel/config/log4j2_client.properties
  }
}
```

声明 `data` 时由 SeaTunnel 创建 ConfigMap。创建型 ConfigMap 命名为 `<application-id>-<entry>`，带 Job ownerReference，随 application 一起清理：

```hocon
kubernetes.configmap.runtime {
  data {
    seatunnel.yaml = "engine { ... }"
  }
  mount {
    seatunnel.yaml = /opt/seatunnel/config/seatunnel.yaml
  }
}
```

两种模式都必须在 `mount` 中为每个挂载的 key 声明完整的容器内路径，SeaTunnel 不假设任何配置目录。

命令行参数可能被提交主机上的其他用户看到，因此 `-ikey=value` 只用于非敏感覆盖。将 `kubernetes.kubeconfig` 和其他敏感部署值写入权限受限的部署配置文件，或使用 Kubernetes SDK 默认凭据。

## 容量规划

Pod request 与 limit 相同。需要同时规划 Worker 数、slot 数、作业并行度、Namespace ResourceQuota、LimitRange 和节点容量。Master 不提供 task slot。
