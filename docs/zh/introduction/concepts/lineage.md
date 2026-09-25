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

每个配置项独立按照以下优先级解析：

```text
作业 env {} > 进程环境变量 > 集群配置 > 默认值
```

Token 是例外：`openlineage_auth_token` 按 `OPENLINEAGE_AUTH_TOKEN` > 集群配置解析。作业
`env {}` 中出现 token 会在启动时直接拒绝。这样可以避免 token 持久化到作业配置，或被序列化到
Flink JobGraph。

血缘投递无条件是 best-effort，并且刻意不提供改变该行为的配置项。接收端或网络失败会在引擎
集成边界被捕获并记录 warning，因此不可能改变数据处理作业的结果。

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
