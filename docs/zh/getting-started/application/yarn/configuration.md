---
sidebar_position: 5
title: 配置参考
---

# YARN 配置参考

部署文件（`-a`）包含 `application.*` 和 `yarn.*` 选项。作业内容可以来自 `-c`，也可以通过 `yarn.job-config-file` 选用本地文件、HDFS 文件或部署文件中内联的内容。首次提交可参考[快速开始](quick-start.md)。

## 配置来源

| 来源 | 使用位置 | 用途 |
| --- | --- | --- |
| `-a yarn.conf` | 提交端 | HOCON 部署选项。通过命令行给出所需选项时可以省略。 |
| `-Dkey=value` | 提交端 | 覆盖部署文件。每个参数设置一个选项。 |
| `-c job.conf` 或 `yarn.job-config-file` | AM | 作业内容，变量占位符在 AM 环境中解析。两者同时提供时，`yarn.job-config-file` 优先。 |
| `yarn.config-dir` 或 `HADOOP_CONF_DIR` | Hadoop 客户端 | 提供集群地址、HDFS nameservice 和认证方式。解析后的 Hadoop 配置会分发给两个角色。 |

## 参数参考

### YARN 选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `yarn.deployment-target` | Enum | 否 | `APPLICATION` | 部署拓扑；一期仅支持每个作业对应一个独立 application。 |
| `yarn.distribution` | String | 提交时 | — | 提交用户可读的 `.tar.gz`、`.tgz` 或 `.zip` 发行包。可填写本地路径、`file:///` URI 或 `hdfs://` URI。 |
| `yarn.config-dir` | String | 否 | 空 | Hadoop 配置目录；为空时使用 `HADOOP_CONF_DIR`，再回退到 Hadoop classpath。 |
| `yarn.staging-dir` | String | 否 | `.seatunnel/applications` | 共享文件系统 staging 根目录；相对路径通常位于提交用户的 HDFS home。 |
| `yarn.queue` | String | 否 | `default` | 提交 application 的 YARN 队列。 |
| `yarn.priority` | Integer | 否 | `-1` | `-1` 沿用集群默认值，可用非负整数指定应用调度优先级。 |
| `yarn.tags` | String | 否 | 空 | 以逗号分隔的 YARN application tag。 |
| `yarn.master.node-label` | String | 否 | 空 | ApplicationMaster 使用的 node-label expression。 |
| `yarn.master.http-port` | Integer | 否 | 未设置 | 在 AM 上启用 Web UI 和 REST API，绑定指定端口（`1` 到 `65535`）。省略时关闭 HTTP。 |
| `yarn.worker.node-label` | String | 否 | 空 | Worker 使用的 node-label expression；为空时继承 `yarn.master.node-label`。 |
| `yarn.hadoop-user-name` | String | 否 | 当前有效 Hadoop 用户的短用户名 | simple 模式下用于 YARN 请求、HDFS staging、AM 和 worker 的用户。 |
| `yarn.active-deadline-seconds` | Long | 否 | 队列策略 | 从提交开始计算的应用运行时限，单位秒，必须为正数。ResourceManager 负责执行，队列可设置上限。 |
| `yarn.job-config-file` | String | 否 | 未设置 | 作业的本地路径、HDFS URI 或已声明的本地化目标路径，仅分发给 AM。设置后可省略 `-c`，两者同时提供时优先使用此文件。 |
| `yarn.kerberos.principal` | String | 否 | 空 | 提交端 keytab 中的 principal。 |
| `yarn.kerberos.keytab` | String | 否 | 空 | 提交机上的 keytab 文件路径。 |
| `yarn.env.<NAME>` | String | 否 | 未设置 | 为 AM 和 worker 设置环境变量，值按字面传递。 |
| `yarn.ship-files.<alias>` | String | 否 | 未设置 | 将本地或 HDFS 文件分发到 AM 和 worker 的 `files/<alias>`。 |
| `yarn.local-resources.<group>.source-dir` | String | 否 | 未设置 | 提交机本地目录或 HDFS 目录，使用同组的 `localize` 选择文件。 |
| `yarn.local-resources.<group>.content.<file>` | String | 否 | 未设置 | 直接写入文件的 UTF-8 文本，多行内容可用 HOCON 三引号。 |
| `yarn.local-resources.<group>.localize.<file>` | String | 目录来源必填 | 内联文件默认 `files/<file>` | 容器工作目录下以 `files/` 开头的目标路径，支持改名和子目录。 |
| `yarn.inline-files.<alias>` | String | 否 | 未设置 | 直接用文本创建 `files/<alias>` 的简写。 |
| `yarn.jvm-options-file` | String | 否 | `jvm_options` | 两端共用的 JVM 参数文件，先于角色参数文件加载。 |
| `yarn.master.jvm-options-file` | String | 否 | `jvm_master_options` | AM 专用 JVM 参数文件。 |
| `yarn.worker.jvm-options-file` | String | 否 | `jvm_worker_options` | Worker 专用 JVM 参数文件。 |
| `yarn.master.hazelcast-config` | String | 否 | 引擎默认值 | AM 的 Hazelcast 成员配置，支持 `.yaml`、`.yml` 和 `.xml`。 |
| `yarn.worker.hazelcast-config` | String | 否 | 引擎默认值 | Worker 的 Hazelcast 成员配置，支持 `.yaml`、`.yml` 和 `.xml`。 |
| `yarn.log4j-config-file` | String | 否 | `log4j2_client.properties` | AM 和 worker 共用的 Log4j2 配置。标准输出、标准错误以及默认的文件日志目录均可由 YARN 聚合。 |
| `yarn.seatunnel-config-file` | String | 否 | `config/seatunnel.yaml` | AM 和 worker 共用的引擎 YAML，可在其中配置 checkpoint 存储。 |

### Application 公共选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `application.name` | String | 否 | `seatunnel` | YARN application 显示名称。 |
| `application.job-id` | Long | 否 | 自动生成 | 正数原生 Zeta job ID。 |
| `application.restore-job-id` | Long | 否 | 未设置 | 从此历史 job ID 的最新有效 checkpoint 恢复。 |
| `application.worker-count` | Integer | 否 | `1` | 固定 Worker Container 数量。 |
| `application.worker.memory-mb` | Integer | 否 | `1024` | 每个 Worker 的 Container 内存，单位 MiB。 |
| `application.worker.cpu-cores` | Integer | 否 | `1` | 每个 Worker 的虚拟 CPU 核数。 |
| `application.worker.slots` | Integer | 否 | `2` | 每个 Worker 的固定执行 slot 数。 |
| `application.master.memory-mb` | Integer | 否 | `1024` | ApplicationMaster Container 内存，单位 MiB。 |
| `application.master.cpu-cores` | Integer | 否 | `1` | ApplicationMaster 虚拟 CPU 核数。 |
| `application.master.port` | Integer | 否 | `5801` | Hazelcast 起始端口；冲突时选择后续可用端口。 |
| `application.startup-timeout-millis` | Long | 否 | `120000` | 分别用于等待 AM 启动，以及 Worker 申请和注册。 |

## 发行包与暂存目录

发行包应包含作业所需的插件和驱动。省略 URI 协议时，路径在提交机上解析。本地归档、普通 HDFS 路径和其他 HDFS nameservice 上的归档会复制到本次应用的私有 staging 目录，提交端在那里校验归档布局。

要让多个应用复用同一发行包，可将 `yarn.distribution` 指向与 `yarn.staging-dir` 位于同一 HDFS 文件系统的具名快照，例如 `hdfs://nameservice1/releases/.snapshot/v1/seatunnel.tgz`。提交端直接校验快照中的归档，记录完整路径、大小和修改时间。NodeManager 使用这份快照启动 AM 和 worker。使用该快照的所有应用结束前，请保留快照名称、路径和读取权限。

| 发行包来源 | NodeManager 本地化的文件 | 源文件由谁保留 |
| --- | --- | --- |
| 提交机本地文件 | 应用暂存目录中的私有归档 | 提交者 |
| 普通 HDFS 文件，包括其他 nameservice 上的文件 | 应用暂存目录中的私有归档 | HDFS 文件所有者 |
| 暂存目录所在 HDFS 的具名快照 | 清单中记录的快照文件 | 快照所有者，直到使用它的应用全部结束 |

### 权限

| 身份或操作 | 需要的权限 |
| --- | --- |
| 提交者 | 向指定队列提交应用，读取发行包和源文件，在 `yarn.staging-dir` 下创建应用目录。 |
| ApplicationMaster | 向 ResourceManager 注册、申请 worker 容器、读取暂存资源，在退出时清理本应用的目录。 |
| 容器本地化 | 使用应用凭据读取所声明的 HDFS 资源。选作作业的文件仅分发给 AM。 |
| 恢复提交 | 读取原作业 ID 下的 checkpoint，并向新作业 ID 的目录写入 checkpoint。 |

每个应用的暂存目录权限为 `0700`。可复用的发行包和持久化 checkpoint 应放在该目录之外。快照保留期由发布者管理。

## Kerberos

认证方式由 Hadoop 配置决定。在默认的 `simple` 模式下，提交端、AM 和 worker 使用当前有效 Hadoop 用户的短用户名。例如，网关进程以 `svc` 运行并设置 `HADOOP_USER_NAME=etl` 时，提交作业和创建 staging 的身份都是 `etl`。显式设置 `yarn.hadoop-user-name` 会为三者统一选择该用户。simple 模式会忽略 Kerberos 选项和 keytab 路径。

集群使用 `hadoop.security.authentication=kerberos` 时，在 Hadoop 配置中设置 `yarn.resourcemanager.principal`。可以使用已有的 `kinit` 票据，或在提交机的部署文件中同时指定以下两个选项。

```hocon
yarn.kerberos.principal = "seatunnel@EXAMPLE.COM"
yarn.kerberos.keytab = "/etc/security/keytabs/seatunnel.keytab"
```

提交端为 staging 所在的 HDFS 获取 delegation token，ResourceManager 可在 HDFS 允许的最大有效期内续期。AM 将 HDFS token 转交 worker，并将自身与 ResourceManager 通信的 token 保留在 AM 内。keytab 仅在提交端使用，查询和取消作业时沿用有权限的身份。

## 调度

队列、优先级和 tags 作用于整个应用。AM 和 worker 可使用不同的节点标签。`yarn.active-deadline-seconds` 从提交时开始计算整个应用的运行时限，由 ResourceManager 按队列策略执行。`application.startup-timeout-millis` 限制 AM 启动以及 worker 分配、注册的等待时间。`--wait-timeout` 只控制 CLI 轮询时间。

AM 和 worker 运行在不同节点组时，可分别设置节点标签。省略 worker 标签会继承 AM 标签。例如两个 worker 各申请 2 GiB 和一个 vCore，AM 申请 1 GiB 和一个 vCore，队列需要容纳三个容器，合计 5 GiB、三个 vCore，实际分配量还取决于调度器的分配粒度。

```hocon
yarn {
  queue = analytics
  priority = 1
  tags = "orders,nightly"
  master.node-label = control
  worker.node-label = batch
}
```

## AM 的 HTTP 端口

```hocon
yarn.master.http-port = 8095
```

这个选项在 AM 上启动 HTTP 服务，并覆盖本地化 `seatunnel.yaml` 中的 HTTP 端口。AM 绑定指定端口，端口占用会使启动失败。worker 继续关闭 HTTP。`application.master.port` 单独控制 worker 连接 AM 使用的 Hazelcast 端口。

AM 向 YARN 注册 `http://<AM 所在 NodeManager 主机>:8095/`。如果引擎指定了 HTTP 上下文路径，地址也会包含该路径。引擎配置中的基本认证、请求大小限制等 HTTP 设置继续生效。应用运行期间可直接访问 AM 地址打开随发行包提供的 UI，或访问 `/overview` 查询 REST 概览。

ResourceManager 的 Web Application Proxy 可转发 `/proxy/<applicationId>/` 下的请求。发行包中的前端从站点根路径加载 `/assets/` 和 `/overview` 等 API。需要完整页面时，可直连 AM，或者让专用网关域名的根路径指向 AM。外部域名、端口映射和访问权限由集群运维方配置。

## 作业配置

使用 `-c job.conf` 时，本地作业内容随 AM 的私有部署描述传递。设置 `yarn.job-config-file` 时，可以选择本地文件、HDFS 文件或 `local-resources` 声明的文件。选中的作业只以 `job.conf` 分发给 AM。内联作业可在三引号内保留 `${DATA_REGION}` 等占位符，由 AM 解析。

## 本地化资源与运行配置文件

在 `yarn.local-resources` 下按用途命名资源组，例如 `runtime` 或 `worker`。已有配置可以放在本地或 HDFS 目录中，用 `source-dir` 指定目录，再用同组的 `localize` 逐个选择文件。相对目录从提交进程的工作目录解析。需要直接创建文件时，另建一组，在 `content` 中写文件名和内容。提交端按 UTF-8 原样写入。

内联文件默认放在 `files/<文件名>`。可用 `localize` 改名或指定子目录，例如 `files/runtime/engine.yaml`。每个目标路径只能对应一个文件。文件名和目标路径的各级名称以字母或数字开头，可包含点、下划线和连字符。AM 和 worker 都会收到运行配置，选作作业的文件只分发给 AM。单个文件也可使用简写，`ship-files.<alias>` 引用本地路径或 HDFS URI，`inline-files.<alias>` 接受文件原文。

| 运行配置中的引用 | 解析位置 |
| --- | --- |
| `jvm_worker_options` | JVM、日志和 Hazelcast 配置文件以发行包的 `config/` 为基准 |
| `config/seatunnel.yaml` | 引擎 YAML 相对于解压后的发行包根目录 |
| `files/runtime/engine.yaml` | `local-resources` 声明的目标，由 NodeManager 本地化 |
| 绝对路径 | 对应角色可能运行的每台节点上预先安装的同一位置 |

## 环境变量

```hocon
yarn.env {
  DATA_REGION = east
  JOB_PARALLELISM = "2"
}
```

两个 JVM 都会收到这些字面值。作业可以在 `env.parallelism` 中使用 `${JOB_PARALLELISM}`。内联编写作业时，将占位符保留在三引号字符串中，供 AM 解析。容器身份、凭据和本地化目录变量由 Hadoop 提供。在 simple 模式下，可通过 `yarn.hadoop-user-name` 指定 Hadoop 用户。

## JVM 参数、Hazelcast 与日志

JVM、Hazelcast 和 Log4j2 的文件名相对于发行包的 `config/` 目录，引擎配置路径相对于发行包根目录。这些选项也支持已分发的 `files/<alias>`，或目标容器均可访问的绝对路径。发行包包含默认 JVM 参数文件时自动加载，任一 JVM 文件选项均可设为 `disabled` 以跳过该文件。

公共 JVM 参数先加载，然后加载对应角色的参数文件。通过 `yarn.master.hazelcast-config` 和 `yarn.worker.hazelcast-config` 分别设置成员配置，运行时由 YARN provider 填入集群名和实际地址。启用聚合后用 `yarn logs -applicationId "$APP_ID"` 查看容器日志。

## 完整示例

将以下内容保存为 `yarn.conf`。请使用集群中已有的队列、节点标签和 HDFS 文件。

```hocon
application {
  name = "orders-batch"
  worker-count = 2
  worker.slots = 4
}
yarn {
  distribution = "hdfs://nameservice1/releases/seatunnel.tgz"
  job-config-file = "hdfs://nameservice1/jobs/orders.conf"
  config-dir = "/etc/hadoop/conf"
  queue = "analytics"
  master.node-label = "batch"
  worker.node-label = "batch"
  active-deadline-seconds = 3600
  env {
    DATA_REGION = east
  }
  seatunnel-config-file = files/runtime/engine.yaml
  master.hazelcast-config = files/runtime/hazelcast-master.yaml
  worker.hazelcast-config = files/runtime/hazelcast-worker.yaml
  worker.jvm-options-file = files/worker.opts

  local-resources {
    runtime {
      source-dir = "hdfs://nameservice1/seatunnel/conf"
      localize {
        "seatunnel.yaml" = files/runtime/engine.yaml
        "hazelcast-master.yaml" = files/runtime/hazelcast-master.yaml
        "hazelcast-worker.yaml" = files/runtime/hazelcast-worker.yaml
      }
    }

    worker {
      content {
        "worker.opts" = "-Duser.timezone=UTC"
      }
    }
  }
}
```

```bash
bin/seatunnel-application.sh submit -t yarn -a yarn.conf --wait
```

## 容量规划

固定执行容量等于 worker 数乘以每个 worker 的 slot 数。应按 source、transform、sink 产生的 task group 规划容量，同时满足队列和 NodeManager 的资源限制。默认 JVM 堆按容器内存的 75% 计算，下限为 64 MiB。
