---
sidebar_position: 4
title: 配置参考
---

# Kubernetes 配置参考

Application Mode 在提交机上读取两个文件：部署文件（`-a`，包含 `application.*` 和
`kubernetes.*` 选项）与作业文件（`-c`）。本页是部署文件的参考；首次提交见
[快速开始](quick-start.md)，恢复相关见 [Checkpoint 恢复](checkpoint-recovery.md)。

## 配置来源

| 来源 | 配置内容 | 说明 |
| --- | --- | --- |
| 部署文件（`-a`） | `application.*` 和 `kubernetes.*` | 在提交机解析。敏感值写在这里，不要放命令行。 |
| 命令行覆盖（`-i key=value`） | 任意部署选项 | 同样在提交机解析；命令行参数可能被同机其他用户看到。 |
| 作业文件（`-c`） | 作业本身 | 原样带到 Master，包含未解析的 `${VAR}` 占位符。 |
| Pod 环境 | 作业中的 `${VAR}` | Master 上由 `kubernetes.env`、`kubernetes.env-from.*` 和挂载的 Secret/ConfigMap 文件组成。 |

:::caution
命令行覆盖（`-i key=value`）可能被提交主机上的其他用户看到。`kubernetes.kubeconfig` 等敏感值请
写入权限受限的部署文件。
:::

## 选项参考

### Kubernetes 选项

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `kubernetes.namespace` | `default` | 所有 application 资源所在的已有 Namespace。 |
| `kubernetes.image` | 必填 | 包含 SeaTunnel、provider 和作业插件的镜像。 |
| `kubernetes.image-pull-policy` | `IfNotPresent` | `Always`、`IfNotPresent` 或 `Never`。 |
| `kubernetes.image-pull-secrets` | 空 | 用于拉取私有仓库镜像的已有 Secret 名称，多个名称以逗号分隔。 |
| `kubernetes.service-account` | `default` | Master 使用的已有 ServiceAccount。 |
| `kubernetes.seatunnel-home` | `/opt/seatunnel` | 镜像内发行包绝对路径，不能包含 `:`。 |
| `kubernetes.configmap.<entry>` | 未设置 | ConfigMap 挂载条目：`ref` 引用已有 ConfigMap 并按 `subPath` 挂载声明的 key；`data` 则由 SeaTunnel 创建并持有 ConfigMap。可选 `name` 覆盖默认的 `<application-id>-<entry>`，便于其他配置按名字引用。 |
| `kubernetes.secret-ref` | 未设置 | 已有 Secret，只读挂载到 `/etc/seatunnel-secrets/application`，永不创建或删除；由它提供作业配置时，把 `kubernetes.job-config-file` 指向 `<secret-directory>/job.conf`。 |
| `kubernetes.secret.name` | application Job 名称 | SeaTunnel 根据 `kubernetes.secret.data.*` 创建 Secret 时使用的名称。 |
| `kubernetes.secret.data.<KEY>` | 未设置 | SeaTunnel 创建的 application Secret 键值；该 Secret 由 Job 拥有，取消时自动删除。 |
| `kubernetes.secret-directory` | `/etc/seatunnel-secrets/application` | application Secret 只读挂载的绝对路径；由它提供作业配置时，把 `kubernetes.job-config-file` 指到该目录下的文件。 |
| `kubernetes.job-config-file` | 未设置 | 可选的作业配置文件，在解析前替换内联作业配置：`<seatunnel-home>/config` 下的文件名（ConfigMap 友好）或绝对路径（例如 `<secret-directory>/job.conf`）；不设置时使用提交的 `-c` 文件。 |
| `kubernetes.env.<NAME>` | 未设置 | 每个环境变量一行：字面值、`secret:<name>/<key>` 或 `configmap:<name>/<key>`。 |
| `kubernetes.env-from.secret` / `kubernetes.env-from.configmap` | 未设置 | 已有 Secret/ConfigMap，其全部 key 注入为环境变量（等价 Kubernetes `envFrom`）。 |
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
| `kubernetes.master.tolerations` | 空 | Master Pod 的 tolerations，格式为 `;` 分隔的 `key[=value][:Effect]`，例如 `dedicated=seatunnel:NoSchedule`。 |
| `kubernetes.worker.tolerations` | 空 | Worker Pod 的 tolerations，语法与 master 选项一致。 |

### Application 选项

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

:::note
每个 Pod 的 JVM 堆使用内存上限的 75%，剩余空间用于堆外内存和 JVM 开销。
:::

:::note
`application.startup-timeout-millis` 不是作业执行时长限制，也不限制提交。提交器在 API 接受并
启动 Job 后立即返回，镜像拉取慢、准入或调度问题不会使提交命令失败，可通过 `status` 和 Pod
事件观察；Master 使用同一超时等待 Worker。
:::

## 指南

### 镜像与凭据

通过 `kubernetes.image-pull-secrets` 引用已有的私有镜像仓库 Secret。

:::note
Provider 不创建这类凭据，也不提供任意 Pod template。Connector 引用的本地文件和 JAR 不会动态
下载，必须已经存在于镜像中。
:::

### 调度

用 `kubernetes.master.node-selector` / `kubernetes.worker.node-selector` 指定节点池，配合
`kubernetes.*.tolerations` 容忍节点池上的 taint。Worker 是 master 运行时创建的，所以 worker 的
tolerations 会随本地化配置一起进入 Pod：

```hocon
kubernetes {
  master.node-selector = "pool:seatunnel"
  worker.node-selector = "pool:seatunnel"
  master.tolerations = "dedicated=seatunnel:NoSchedule"
  worker.tolerations = "dedicated=seatunnel:NoSchedule;spot:NoExecute"
}
```

每个条目形如 `key[=value][:Effect]`，多个用 `;` 分隔：不带 value 表示 `Exists` 操作符；显式
effect 只能是 `NoSchedule`、`PreferNoSchedule` 或 `NoExecute`。不配 tolerations 时，Pod 会在带
taint 的节点池上一直 `Pending`，事件里是 `had untolerated taint`。

### 作业配置

`-c job.conf` 在提交机上是**原样读取**并放入本地化配置的，提交端只做一次“允许占位符未解析”的
解析检查。Master 启动时，如果配置了 `kubernetes.job-config-file`，选定的文件会在解析作业前整体
替换它。该选项默认是 `null`：不配置就不挂载、不读取任何文件。因此最终生效的是：

1. 不设置（默认）或设为空值：始终使用提交的 `-c` 文件；
2. 设置了路径且文件存在——`<seatunnel-home>/config` 下的文件名，或任意挂载了 Secret 或
   ConfigMap key 的绝对路径：使用该挂载文件；
3. 设置了路径但文件不存在：Master 打印 WARN 并回退到 `-c` 的内容。

最终文本只在 Master 解析一次，`${VAR}` 在此时结合 Pod 环境解析，所以提交机的值不会被烘进作业。

**ConfigMap（推荐）**：把 `kubernetes.job-config-file` 设为相对文件名，并把该 key 挂到
`<seatunnel-home>/config` 下，不需要任何 Secret 权限：

```hocon
kubernetes {
  job-config-file = job.conf
  configmap.job-conf {
    ref = platform-job-config
    mount {
      job.conf = /opt/seatunnel/config/job.conf
    }
  }
}
```

**Secret**：保留 Secret 挂载，并显式指向挂载出来的文件：

```hocon
kubernetes {
  secret-ref = platform-job-secrets
  job-config-file = /etc/seatunnel-secrets/application/job.conf
}
```

:::tip
直接写在部署文件里的值会在提交机解析；但写在引号（或三引号）字符串里的 `${VAR}` 会被原样保留，
因此由 SeaTunnel 创建的 Secret/ConfigMap data 也可以承载带 Master 端占位符的作业文件。
:::

### Secret

application Secret 有两种互斥模式。两种模式都会把 Secret 只读挂载到 Master 和 Worker Pod 的
`kubernetes.secret-directory`（默认 `/etc/seatunnel-secrets/application`）；需要由 Secret 提供
作业配置时，把 `kubernetes.job-config-file` 指到 `<secret-directory>/job.conf`。

| 模式 | 配置 | 生命周期 |
| --- | --- | --- |
| 引用 | `kubernetes.secret-ref` | 由平台创建和管理；SeaTunnel 永不创建或删除 |
| 创建 | `kubernetes.secret.name` / `kubernetes.secret.data.*` | 由 SeaTunnel 创建并持有；带 Job ownerReference，随 application 一起删除 |

**引用已有 Secret**：SeaTunnel 只挂载、不修改。作业配置里使用 `${VAR}` 时，通过
`kubernetes.env` 把 key 注入为 Pod 环境变量：

```hocon
kubernetes {
  secret-ref = platform-job-secrets
  env.MYSQL_PASSWORD = "secret:platform-job-secrets/password"
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
  env {
    MYSQL_PASSWORD = "secret:seatunnel-job-secrets/MYSQL_PASSWORD"
    S3_SECRET_KEY = "secret:seatunnel-job-secrets/S3_SECRET_KEY"
  }
}
```

创建型 Secret 也可以承载作业文件；由于默认路径指向 `<seatunnel-home>/config`，需要显式选择：

```hocon
kubernetes {
  secret {
    name = seatunnel-job-secrets
    data {
      job.conf = """
        env {
          job.mode = "BATCH"
          parallelism = 2
        }
        source {
          FakeSource {
            row.num = 10
            schema { fields { id = int } }
          }
        }
        sink {
          Console {}
        }
        """
    }
  }
  secret-directory = /etc/seatunnel-secrets/application
  job-config-file = /etc/seatunnel-secrets/application/job.conf
}
```

:::caution
凭据不要放进 ConfigMap：它不具备保密性。作业需要凭据时使用 Secret + `kubernetes.env` /
`${VAR}` 注入。
:::

### ConfigMap 与运行时文件

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

**由 SeaTunnel 创建全部运行时文件**：`data` 支持 YAML、properties 和普通文本，每个文件用三引号
字符串书写并挂到容器实际读取的路径。下面的例子一次性替换引擎配置、Hazelcast 主/从配置、日志、
JVM 参数和作业文件：

```hocon
kubernetes {
  job-config-file = job.conf
  configmap {
    runtime {
      name = seatunnel-runtime
      data {
        seatunnel.yaml = """
          seatunnel:
            engine:
              classloader-cache-mode: true
              checkpoint:
                interval: 10000
                timeout: 60000
          """
        hazelcast-master.yaml = """
          hazelcast:
            cluster-name: seatunnel
            network:
              port:
                auto-increment: false
                port: 5801
          """
        hazelcast-worker.yaml = """
          hazelcast:
            cluster-name: seatunnel
            network:
              port:
                auto-increment: true
                port: 5802
          """
        log4j2_client.properties = """
          rootLogger.level = INFO
          rootLogger.appenderRef.console.ref = console
          appender.console.type = Console
          appender.console.name = console
          """
        jvm_options = """
          -XX:+UseG1GC
          -XX:MaxMetaspaceSize=2g
          """
        job.conf = """
          env {
            job.mode = "BATCH"
            parallelism = 2
          }
          source {
            FakeSource {
              plugin_output = "example"
              row.num = 10
              schema { fields { id = int } }
            }
          }
          sink {
            Console {
              plugin_input = "example"
            }
          }
          """
      }
      mount {
        seatunnel.yaml = /opt/seatunnel/config/seatunnel.yaml
        hazelcast-master.yaml = /opt/seatunnel/config/hazelcast-master.yaml
        hazelcast-worker.yaml = /opt/seatunnel/config/hazelcast-worker.yaml
        log4j2_client.properties = /opt/seatunnel/config/log4j2_client.properties
        jvm_options = /opt/seatunnel/config/jvm_options
        job.conf = /opt/seatunnel/config/job.conf
      }
    }
  }
  master.hazelcast-config = hazelcast-master.yaml
  worker.hazelcast-config = hazelcast-worker.yaml
}
```

:::note
引用型 ConfigMap 不会被修改或删除，挂载的 key 会在提交前校验；如果提交身份没有读取权限，校验会
降级为告警，Pod 仍通过自己的 ServiceAccount 挂载。
:::

:::note
创建型 ConfigMap 的名字为 `name` 或 `<application-id>-<entry>`，带 Job ownerReference，随
application 一起删除；其他选项需要按名字引用它时必须设置 `name`。
:::

:::tip
每个 key 都通过 `subPath` 单独挂载，因此 ConfigMap 不会遮蔽镜像在同一目录下自带的其他文件；
引用与创建条目之间不允许出现相同的挂载路径。
:::

运行时文件路径由各选项选择：`kubernetes.seatunnel-config-file`（引擎配置）、
`kubernetes.master.hazelcast-config` / `kubernetes.worker.hazelcast-config`、
`kubernetes.jvm-options-file` 及角色文件、`kubernetes.log4j-config-file`、
`kubernetes.job-config-file`。

### 环境变量

`kubernetes.env.<NAME>` 每个变量一行，值可以是字面量、Secret key 引用 `secret:<name>/<key>` 或
ConfigMap key 引用 `configmap:<name>/<key>`；`kubernetes.env-from.secret` /
`kubernetes.env-from.configmap` 则一次性注入已有对象的全部 key：

```hocon
kubernetes {
  env {
    JOB_MODE = "BATCH"                                               # 字面量
    MYSQL_PASSWORD = "secret:platform-job-secrets/password"          # secretKeyRef
    ENGINE_FILE = "configmap:platform-engine-config/seatunnel.yaml"  # configMapKeyRef
  }
  env-from.secret = platform-extra-secrets
}
```

注入的值会进入 Master Pod 的环境，作业配置（内联或挂载文件）里的 `${VAR}` 就在那里解析。

:::note
同名时显式 `env.<NAME>` 优先于 `env-from`；Kubernetes 会跳过 `envFrom` 中不是合法环境变量名的 key。
:::

### JVM 参数与日志

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
或设为 `none` 跳过文件。

:::note
Hazelcast 和 Kerberos 配置重载所需的 Java 11+ 强制模块参数（`--add-opens` / `--add-exports`）
始终会被追加，挂载的文件只做追加或覆盖。
:::

日志由 `kubernetes.log4j-config-file` 决定（默认 `log4j2_client.properties`，使用 console
appender，`kubectl logs` 可直接查看）。设为 `log4j2.properties` 可切换到发行版的服务端写文件配置，
并把该文件挂到对应路径；日志会写到 `<seatunnel-home>/logs`。

## 完整示例

```hocon
application {
  name = "example-batch"
  worker-count = 2
  master.memory-mb = 1024
  master.cpu-cores = 1
  worker.memory-mb = 2048
  worker.cpu-cores = 2
  worker.slots = 4
  startup-timeout-millis = 180000
}

kubernetes {
  namespace = seatunnel
  image = "registry.example.com/seatunnel/application:3.0.1"
  image-pull-policy = IfNotPresent
  service-account = seatunnel-application
  job-config-file = job.conf

  # 平台管理的运行时文件；job.conf 由上面的选项显式选中
  configmap {
    runtime {
      ref = platform-runtime-config
      mount {
        seatunnel.yaml = /opt/seatunnel/config/seatunnel.yaml
        log4j2_client.properties = /opt/seatunnel/config/log4j2_client.properties
        hazelcast-master.yaml = /opt/seatunnel/config/hazelcast-master.yaml
        hazelcast-worker.yaml = /opt/seatunnel/config/hazelcast-worker.yaml
        jvm_options = /opt/seatunnel/config/jvm_options
        job.conf = /opt/seatunnel/config/job.conf
      }
    }
  }

  # 凭据放在 Secret 里，作业用 ${VAR} 引用
  secret-ref = platform-job-secrets
  env {
    MYSQL_PASSWORD = "secret:platform-job-secrets/password"
  }

  master.hazelcast-config = hazelcast-master.yaml
  worker.hazelcast-config = hazelcast-worker.yaml
  checkpoint-pvc = seatunnel-checkpoints
  finished-job-retention-seconds = 86400
}
```

```bash
bin/seatunnel-application.sh submit --target kubernetes -c job.conf -a kubernetes.conf
```

## 容量规划

:::caution
Pod request 与 limit 相同。需要同时规划 Worker 数、slot 数、作业并行度、Namespace
ResourceQuota、LimitRange 和节点容量。Master 不提供 task slot。
:::
