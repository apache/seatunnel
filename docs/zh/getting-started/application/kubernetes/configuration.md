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
| `kubernetes.configmap.<entry>` | 未设置 | ConfigMap 挂载条目：`ref` 引用已有 ConfigMap 并按 `subPath` 挂载声明的 key；`data` 则由 SeaTunnel 创建并持有 ConfigMap。可选 `name` 覆盖默认的 `<application-id>-<entry>`，便于其他配置按名字引用。 |
| `kubernetes.secret-ref` | 未设置 | 已有 Secret，只读挂载到 `/etc/seatunnel-secrets/application`，永不创建或删除；其中 `job.conf` 键会替换内联的作业配置。 |
| `kubernetes.secret.name` | application Job 名称 | SeaTunnel 根据 `kubernetes.secret.data.*` 创建 Secret 时使用的名称。 |
| `kubernetes.secret.data.<KEY>` | 未设置 | SeaTunnel 创建的 application Secret 键值；该 Secret 由 Job 拥有，取消时自动删除。 |
| `kubernetes.secret-directory` | `/etc/seatunnel-secrets/application` | application Secret 只读挂载的绝对路径；该目录中的 `job.conf` 会替换内联的作业配置。 |
| `kubernetes.env` | 空 | 注入 Master 和 Worker 容器的环境变量条目，逗号分隔：`NAME=value`、`NAME=secret:<name>/<key>` 或 `NAME=configmap:<name>/<key>`。 |
| `kubernetes.kubeconfig` | SDK 默认 | 提交端本地 kubeconfig；不会分发到 Master。应写入权限受限的部署配置文件，不要通过 `-i` 传递。 |
| `kubernetes.finished-job-retention-seconds` | `86400` | 完成或失败 Job 的保留秒数，必须为正数。 |
| `kubernetes.checkpoint-pvc` | 未设置 | 已有 PVC，挂载到 Master 的 `kubernetes.checkpoint-directory`。 |
| `kubernetes.checkpoint-directory` | `/opt/seatunnel/checkpoints` | checkpoint PVC 在 Master 上的绝对挂载路径。 |
| `kubernetes.master.jvm-options-file` | `jvm_master_options` | Master 的 JVM 参数文件：`<seatunnel-home>/config` 下的文件名或绝对路径，通过 Java `@argfile` 语法生效；设为 `none` 可跳过。 |
| `kubernetes.worker.jvm-options-file` | `jvm_worker_options` | Worker 的 JVM 参数文件，取值形式与 Master 相同。 |
| `kubernetes.jvm-options-file` | `jvm_options` | 公共 JVM 参数文件，先于角色文件应用于 Master 和 Worker；设为 `none` 可跳过。 |
| `kubernetes.log4j-config-file` | `log4j2_client.properties` | 传给 `-Dlog4j2.configurationFile` 的 Log4j2 配置：`<seatunnel-home>/config` 下的文件名、绝对路径，或 `none`。默认保持 console 日志；设为 `log4j2.properties` 使用写文件的服务端配置。 |
| `kubernetes.seatunnel-config-file` | `config/seatunnel.yaml` | 传给 `-Dseatunnel.config` 的引擎配置文件：相对 `<seatunnel-home>` 的路径、绝对 `.yaml` 路径，或 `none` 禁用该参数。 |
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

通过 `kubernetes.configmap.<entry>` 将 `seatunnel.yaml`、`log4j2_client.properties` 等运行时配置文件放入 Master 和 Worker Pod。每个 key 按其 `mount` 声明的路径以只读方式挂载，因此镜像在 `<seatunnel-home>/config` 下自带的其他文件（包括 `jvm_options`）会保持原样。

默认情况下，本地化后的 application 配置（含作业配置）通过 Master 容器的环境变量传递，因此提交不需要创建 Secret 的权限。`kubernetes.secret-ref` 只读引用平台管理的已有 Secret，`kubernetes.secret.data.*` 则由 SeaTunnel 创建并持有 Secret；两种方式中若包含 `job.conf` 键，会替换内联的作业配置。引用型 Secret 不会被修改或删除，创建型 Secret 带 Job ownerReference，随 application 一起清理。请把 Connector 凭据放在平台管理的 Secret 中，不要内联到本地化配置。

作业配置会原样带到 Master，所有 `${VAR}` 占位符都在 Master 内结合 Pod 环境解析，包括通过 `kubernetes.env` 的 `secretKeyRef`、`configMapKeyRef` 注入的值。提交端不再替换作业变量；任何环境都不存在的占位符会在 Master 解析作业时报错，而不是提交时。请把凭据从作业文件中移出，用 `${VAR}` 注入。

### Application Secret

application Secret 有两种互斥模式。两种模式都会把 Secret 只读挂载到 Master 和 Worker Pod 的
`kubernetes.secret-directory`（默认 `/etc/seatunnel-secrets/application`）；Secret 里如果包含
`job.conf`，会替换内联的作业配置。

| 模式 | 配置 | 生命周期 |
| --- | --- | --- |
| 引用 | `kubernetes.secret-ref` | 由平台创建和管理；SeaTunnel 永不创建或删除 |
| 创建 | `kubernetes.secret.name` / `kubernetes.secret.data.*` | 由 SeaTunnel 创建并持有；带 Job ownerReference，随 application 一起删除 |

**引用已有 Secret**：SeaTunnel 只挂载、不修改。作业配置里使用 `${VAR}` 时，通过
`kubernetes.env` 把 key 注入为 Pod 环境变量：

```hocon
kubernetes {
  secret-ref = platform-job-secrets
  env = "MYSQL_PASSWORD=secret:platform-job-secrets/password"
}
```

**由 SeaTunnel 创建 Secret**：`data` 在提交机解析后写入 Opaque Secret，默认名称为 application
ID，可用 `name` 覆盖；创建出的 key 通过 `kubernetes.env` 注入：

```hocon
kubernetes {
  secret {
    name = seatunnel-job-secrets
    data {
      MYSQL_PASSWORD = ${MYSQL_PASSWORD}   # 在提交机解析
      S3_SECRET_KEY = "s3-secret-key"
    }
  }
  env = "MYSQL_PASSWORD=secret:seatunnel-job-secrets/MYSQL_PASSWORD,S3_SECRET_KEY=secret:seatunnel-job-secrets/S3_SECRET_KEY"
}
```

- 两种模式只能配置一种；都不配置时不创建、不引用，也不会报错。
- Secret 的 namespace 始终跟随 `kubernetes.namespace`。
- 提交机解析不了的 `${VAR}` 会保留在作业配置中，由 Master 从注入的环境变量里解析。

### Application ConfigMap

每个 `kubernetes.configmap.<entry>` 条目都会在 Master 和 Worker Pod 中按 `subPath` 挂载一个或
多个文件。每个挂载的 key 都必须在 `mount` 中声明完整的容器内路径，SeaTunnel 不假设任何配置目录。

| 模式 | 配置 | 生命周期 |
| --- | --- | --- |
| 引用 | `ref` + `mount` | 由平台创建和管理；SeaTunnel 只挂载，并在提交前校验引用的 key |
| 创建 | `name`（可选）+ `data` + `mount` | SeaTunnel 按 `name` 或 `<application-id>-<entry>` 创建并持有；带 Job ownerReference，随 application 一起删除 |

**挂载已有 ConfigMap 的文件**：每个 ConfigMap 一个条目，就可以把不同 ConfigMap 的文件挂到不同路径：

```hocon
kubernetes.configmap {
  engine-config {
    ref = platform-engine-config
    mount {
      seatunnel.yaml = /opt/seatunnel/config/seatunnel.yaml
    }
  }
  logging-config {
    ref = platform-logging-config
    mount {
      log4j2_client.properties = /opt/seatunnel/config/log4j2_client.properties
    }
  }
}
```

**由 SeaTunnel 创建 ConfigMap**：如果其他配置需要引用它（例如 `configMapKeyRef` 环境变量），
必须显式设置 `name`；不设置时名字为 `<application-id>-<entry>`：

```hocon
kubernetes {
  configmap.runtime {
    name = seatunnel-runtime-config
    data {
      seatunnel.yaml = """
        engine:
          checkpoint:
            interval: 10000
        """
    }
    mount {
      seatunnel.yaml = /opt/seatunnel/config/seatunnel.yaml
    }
  }
  env = "ENGINE_CONFIG=configmap:seatunnel-runtime-config/seatunnel.yaml"
}
```

- 引用型 ConfigMap 不会被修改或删除，挂载的 key 会在提交前校验；如果提交身份没有读取权限，
  校验会降级为告警，Pod 仍通过自己的 ServiceAccount 挂载。
- 每个 key 都通过 `subPath` 单独挂载，因此 ConfigMap 不会遮蔽镜像在同一目录下自带的其他文件。
- `seatunnel.yaml` 由 `kubernetes.seatunnel-config-file` 指定（默认 `config/seatunnel.yaml`，相对
  `<seatunnel-home>`）；把替换文件挂到该路径即可修改引擎配置。

### JVM options

Application mode 会把发行版的 JVM 参数文件通过 Java `@argfile` 语法传给 JVM：先加载公共的
`jvm_options`，Master 容器再加载 `jvm_master_options`，Worker 容器再加载 `jvm_worker_options`。
用 `kubernetes.configmap` 挂载替换文件后即可生效，不需要其他改动：

```hocon
kubernetes.configmap.jvm-options {
  ref = platform-jvm-options
  mount {
    jvm_master_options = /opt/seatunnel/config/jvm_master_options
    jvm_worker_options = /opt/seatunnel/config/jvm_worker_options
  }
}
```

也可以通过 `kubernetes.master.jvm-options-file` / `kubernetes.worker.jvm-options-file` 指向绝对路径，
或设为 `none` 跳过文件。Hazelcast 和 Kerberos 配置重载所需的 Java 11+ 强制模块参数
（`--add-opens` / `--add-exports`）始终会被追加，挂载的文件只做追加或覆盖。

日志由 `kubernetes.log4j-config-file` 决定（默认 `log4j2_client.properties`，使用 console
appender，`kubectl logs` 可直接查看）。设为 `log4j2.properties` 可切换到发行版的服务端写文件配置，
并把该文件挂到对应路径；日志会写到 `<seatunnel-home>/logs`。

命令行参数可能被提交主机上的其他用户看到，因此 `-ikey=value` 只用于非敏感覆盖。将 `kubernetes.kubeconfig` 和其他敏感部署值写入权限受限的部署配置文件，或使用 Kubernetes SDK 默认凭据。

## 容量规划

Pod request 与 limit 相同。需要同时规划 Worker 数、slot 数、作业并行度、Namespace ResourceQuota、LimitRange 和节点容量。Master 不提供 task slot。
