---
title: OpenLineage 血缘上报
---

# OpenLineage 血缘上报

SeaTunnel 可以向任意兼容 OpenLineage 的接收端发射表级 `RunEvent` 记录。该能力默认关闭，
关闭时不会创建血缘网络请求或血缘专用后台线程，现有作业行为保持不变。

## 配置项

除特别说明外，以下配置项可以写在作业的 `env` 块中。

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `openlineage_enabled` | Boolean | `false` | 是否启用 OpenLineage 上报。 |
| `openlineage_transport` | String | `http` | 传输后端名称，对应 `LineageBackend` SPI。 |
| `openlineage_url` | String | 无 | OpenLineage 接收端基址。启用上报时必须配置。 |
| `openlineage_namespace` | String | `seatunnel` | OpenLineage job namespace。 |
| `openlineage_auth_token` | String | 无 | 接收端 bearer token，只能来自环境变量或集群配置。 |
| `openlineage_timeout_ms` | Int | `10000` | 单次发送尝试的超时时间，单位为毫秒。 |
| `openlineage_retry_times` | Int | `3` | 发送失败后的重试次数。 |
| `openlineage_run_facet` | String | `seatunnel_properties` | 自定义 run facet 的名称。 |
| `openlineage_run_properties` | Map | 无 | 写入 run facet 的自定义属性。 |
| `openlineage_heartbeat_min_interval_ms` | Long | `3600000` | 流作业心跳的最小间隔，单位为毫秒。设置为 `0` 表示关闭心跳上报。 |
| `openlineage_producer` | String | `https://seatunnel.apache.org/<version>` | OpenLineage producer 标识。默认值在运行时根据当前 SeaTunnel 版本生成。 |
| `openlineage_job_name_per_output` | Boolean | `false` | 仅适用于 Zeta。每个输出使用独立的 job name `<job name>::<输出 namespace>/<输出 name>`，而不是共用作业名。接收端对每个 job 只保留一个当前 run 时，多输出作业否则只会显示其中一个输出。 |

每个配置项独立按照以下优先级解析：

```text
作业 env {} > 进程环境变量 > 集群配置 > 默认值
```

Token 是例外：`openlineage_auth_token` 按 `OPENLINEAGE_AUTH_TOKEN` > 集群配置解析。
`LineageConfig.resolve` 在解析血缘配置时拒绝作业 `env {}` 中的 token。引擎集成必须在持久化
作业配置或将其序列化到 Flink JobGraph 之前执行此校验，即使血缘上报未启用也不例外。
仅声明选项规则不会执行此校验。

配置 token 后，接收端 URL 和 transport 按进程环境变量、集群配置、默认值解析。
作业中的 URL 或 transport 必须与这些可信值一致，否则配置解析失败。请将接收端 URL
与 token 一起放入可信配置，不要仅在作业中配置 URL。Flink 在 JobManager 重新解析凭据时
也执行此校验，token 仍不会进入序列化的 JobGraph。未配置 token 时保留作业覆盖接收端的能力；
这项保护不等于对所有出站网络地址的通用限制。

血缘投递无条件是 best-effort，并且刻意不提供改变该行为的配置项。接收端或网络失败会在引擎
集成边界被捕获并记录 warning，因此不可能改变数据处理作业的结果。
Zeta 使用两个投递线程，最多允许 64 个作业等待投递，并合并每个作业等待发送的心跳。
队列满时，生命周期事件可替换仅等待心跳的作业；如果没有可替换的作业，新事件会被丢弃并记录日志。
服务关闭或重试耗尽后仍投递失败时也可能丢失事件。


Zeta 已经开始投递的作业在同一线程中排空其待发送生命周期事件。环境或集群配置中的
`openlineage_timeout_ms` 和 `openlineage_retry_times` 同时约束作业覆盖值的上限
（默认 10000 毫秒和 3 次重试），超出上限的作业配置会被拒绝。心跳指标采集使用由有效
timeout 限定的统一 deadline；缺少 worker 指标时记录日志并省略统计，仍可发送心跳。
心跳节流按发送尝试计时，不代表 HTTP 接收确认；仅排队或被驱逐的心跳不会推进节流时间。

### 环境变量名称

环境变量名称是配置项名称的大写形式：

| 配置项 | 环境变量 |
| --- | --- |
| `openlineage_enabled` | `OPENLINEAGE_ENABLED` |
| `openlineage_transport` | `OPENLINEAGE_TRANSPORT` |
| `openlineage_url` | `OPENLINEAGE_URL` |
| `openlineage_namespace` | `OPENLINEAGE_NAMESPACE` |
| `openlineage_auth_token` | `OPENLINEAGE_AUTH_TOKEN` |
| `openlineage_timeout_ms` | `OPENLINEAGE_TIMEOUT_MS` |
| `openlineage_retry_times` | `OPENLINEAGE_RETRY_TIMES` |
| `openlineage_run_facet` | `OPENLINEAGE_RUN_FACET` |
| `openlineage_run_properties` | `OPENLINEAGE_RUN_PROPERTIES` |
| `openlineage_heartbeat_min_interval_ms` | `OPENLINEAGE_HEARTBEAT_MIN_INTERVAL_MS` |
| `openlineage_producer` | `OPENLINEAGE_PRODUCER` |
| `openlineage_job_name_per_output` | `OPENLINEAGE_JOB_NAME_PER_OUTPUT` |

例如，可以通过进程环境或服务管理器注入 `OPENLINEAGE_URL` 和 `OPENLINEAGE_AUTH_TOKEN`。不要把真实
token 写入作业文件、仓库文件、命令历史或日志。

## 数据集命名

支持的连接器数据集标识采用以下规范形式。多表 source 和 sink 会为每张配置的表分别生成
数据集标识：

| 连接器 | Namespace | Name |
| --- | --- | --- |
| Paimon | `paimon://<catalog_name>/<database>` | `<table>` |
| Doris | `mysql://<fe_host>:<query_port>` | `<database>.<table>` |
| JDBC | `<scheme>://<host>:<port>` | `<database>.<table>` |

对于 JDBC extractor 处理的连接器，`base-url` 包含 JDBC URL 时可作为 `url` 的回退配置。
MySQL-CDC 从 `url`（或回退配置 `base-url`）解析连接地址，并将 `table-names` 中每个完整表名按 JDBC 命名规则展开为 dataset。`database-names` 仅过滤订阅范围，不补全或改写这些表名。

Paimon 的 `<catalog_name>` 来自连接器的 `catalog_name` 配置，数据集的 name 只有表名，不是
`database.table`。如果需要生成 Paimon 血缘，应显式设置 `catalog_name`；未设置时不会发射
Paimon 数据集。Doris 的 `fenodes` 通常是 HTTP Stream Load
端口，而血缘 namespace 使用 Doris query port。通用的 `default.default.default` 表路径不会
被发射。

使用模板路由的 sink（例如 `table = "${table_name}"`）同样不会产生数据集。模板在写入时按行替换，
本身从不指向真实的表；若原样发射，所有使用模板路由的作业会在血缘图上汇聚到同一个节点。仅仅
包含美元符号的名称（例如 `orders$archive`）是正常标识符，仍会被发射。

## Run facet 属性

除 `openlineage_run_properties` 配置的属性外，run facet 还会携带 `engine` 属性，标明上报该 run
的执行引擎。各引擎还会补充能在自己 UI 中定位该 run 的标识，见下面各引擎章节。

## Zeta

### Zeta 集群配置

Zeta 从 `seatunnel.yaml` 读取集群级配置：

```yaml
seatunnel:
  engine:
    openlineage:
      enabled: true
      transport: http
      url: http://lineage.example/api/lineage
      namespace: seatunnel
      timeout_ms: 10000
      retry_times: 3
      run_facet: seatunnel_properties
      heartbeat_min_interval_ms: 3600000
      producer: https://seatunnel.apache.org/<version>
```

集群级 `auth_token` 也受支持，但应通过集群的 secret 管理机制提供，不要复制到作业 `env` 块。

### Zeta 上报的事件

Zeta 在作业进入 `RUNNING` 时发射 `START`，并在终态发射对应的 `COMPLETE`、`ABORT` 或 `FAIL`。
`SAVEPOINT_DONE` 映射为 `COMPLETE`，`UNKNOWABLE` 映射为 `FAIL`。run facet 除 `engine` 外还携带
`sink_action` 属性。

Zeta 从历史作业指标读取终态输出统计，优先使用 committed 计数；没有正的 committed 计数时回退到
attempted 写入计数。事件中会记录实际采用的是哪一种口径，因此消费方不会把 attempted 行数误认为
committed 行数。

计数器按 sink 实际解析的写入目标表读取，且只有目标表归属于唯一 sink action 时才使用。
全作业计数仅在作业只有一个 sink 时作为回退。同一 sink 有多个血缘输出、不同 sink 的目标表重叠，
或没有可归属的计数器时省略统计，不会将作业总计复制给每个输出。`FAIL` 和 `ABORT` 不携带输出统计。

未结束的作业会周期性发送心跳事件，使接收端不会把仍在运行的作业误判为 producer 已死。Zeta 搭在
checkpoint 完成回调上发送，因此关闭 checkpoint 的 Zeta 作业没有心跳。心跳受
`openlineage_heartbeat_min_interval_ms` 节流，设置为 `0` 时不再发送。确实停止上报的 run 受接收端
abandoned-run 超时约束，该推断状态会被之后到达的终态事件覆盖。
