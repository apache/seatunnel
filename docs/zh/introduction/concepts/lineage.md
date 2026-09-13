---
title: OpenLineage 血缘上报
---

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

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

## Flink

### Flink 集群配置

Flink 使用 `openlineage.` 前缀读取集群级配置。Flink 1.20 及之后版本使用 `config.yaml`；旧版
`flink-conf.yaml` 文件名也支持：

```yaml
openlineage.enabled: true
openlineage.transport: http
openlineage.url: http://lineage.example/api/lineage
openlineage.namespace: seatunnel
openlineage.timeout_ms: 10000
openlineage.retry_times: 3
openlineage.run_facet: seatunnel_properties
openlineage.heartbeat_min_interval_ms: 3600000
openlineage.producer: https://seatunnel.apache.org/<version>
```

### 在集群上安装血缘产物

Flink 侧只有一个可部署产物，由 `seatunnel-lineage-flink` 模块构建：

```text
seatunnel-lineage-flink-<version>-shaded.jar
```

把它放进 JobManager 的 `$FLINK_HOME/lib` 目录并重启 JobManager —— Flink 的 classpath 在启动时固定。
只放一份，升级时先删掉旧版本：`lib/` 里同时存在两个版本会让实际的 classpath 顺序变得不确定。

该产物把 Jackson、OpenLineage 模型、Apache HttpClient 以及 commons-logging 桥接一并 relocate 到
`org.apache.seatunnel.lineage.shaded` 之下。这一点很关键：`lib/` 位于父 classloader，放进去的类对该
集群上的**所有**作业可见，relocate 才能保证本产物不会改变那些作业看到的 Jackson 版本。不要用
SeaTunnel starter jar 代替它 —— 后者携带的是未 relocate 的同名依赖。

如果安装环境同时支持 Flink 1.20+ 和 legacy 部署，应在实际使用的两种配置文件布局中保持相同配置。

**未安装该产物时，开启血缘的作业会提交失败。** 这是刻意的设计——静默丢失血缘比拒绝启动更糟——
提交错误会指出缺失的类以及解决办法。若要在未安装的情况下提交作业，请设置
`openlineage_enabled=false`。

### Flink 上报的事件

只有运行时提供所需 API 时，Flink 才会注册状态 hook。Flink 侧血缘需要 Flink 1.16 或更高版本；在
SeaTunnel starter 中对应 Flink 1.20 路径，Flink 1.13 和 1.15 starter 不注册该 hook，作业行为
保持不变。

Flink 作业成功结束时只有一侧上报终态事件，因此同一个 run 不会收到两次 `COMPLETE`。attached
提交由客户端上报，因为只有客户端能读到输出统计；detached 提交由 JobManager 的 status hook
上报，不带统计信息。`FAIL` 和 `ABORT` 由 status hook 上报，作业根本没有启动的情况除外：提交
失败时——没有可用 slot、凭据被拒绝，或者 JobManager 无法加载 status hook——由客户端上报
`FAIL`，因为集群从未创建过这个作业，也就不会有 hook 运行。

因此 Flink detached 作业没有 `outputStatistics`，因为 detached 结果不暴露 accumulators。
Attached 执行会发射可获得的 `SinkWriteCount` 和 `SinkWriteBytes`，其口径为 `attempted`。

run facet 除 `engine` 外还携带 `flink_job_id` 属性。runId 在作业提交前就已生成，那时还没有
Flink job ID，因此 `flink_job_id` 是把血缘 run 关联回 Flink UI 与 REST API 中那个作业的唯一
途径。

未结束的作业会周期性发送心跳事件，使接收端不会把仍在运行的作业误判为 producer 已死。Flink 由
JobManager 上的定时任务发送，不依赖 checkpoint。心跳受 `openlineage_heartbeat_min_interval_ms`
节流，设置为 `0` 时不再发送。
