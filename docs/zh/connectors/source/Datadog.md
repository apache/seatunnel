<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->


import ChangeLog from '../changelog/connector-http-datadog.md';

# Datadog

Datadog Source 支持批处理。每个 Source 配置段采集一个资源。一个作业可以配置多个 Source，分别采集不同资源或时间范围。

## 支持的资源

| 资源 | API | 应用密钥所需权限 |
| --- | --- | --- |
| `audit_logs` | GET `/api/v2/audit/events` | `audit_logs_read` |
| `dashboards` | GET `/api/v1/dashboard` | `dashboards_read` |
| `downtimes` | GET `/api/v1/downtime` | `monitors_read` |
| `incident_teams` | GET `/api/v2/team` | `teams_read` |
| `incidents` | GET `/api/v2/incidents` | `incident_read` |
| `logs` | GET `/api/v2/logs/events` | `logs_read_data` |
| `metrics` | GET `/api/v2/metrics` | `metrics_read` |
| `monitors` | GET `/api/v1/monitor` | `monitors_read` |
| `service_level_objectives` | GET `/api/v1/slo` | `slos_read` |
| `synthetic_tests` | GET `/api/v1/synthetics/tests` | `synthetics_read` |
| `users` | GET `/api/v2/users` | `user_access_read` |

仪表盘接口提供摘要，指标接口提供指标目录和可用的元数据。停机计划使用 v1 接口，一次返回一个数组。
监控器使用 v1 ID 偏移分页。这两种模式同样受单个响应大小的限制。事件管理接口目前处于公开测试阶段。
可用性、数据可见范围和保留期由 Datadog 站点及组织权限决定。

## 配置项

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `resource` | String | 是 | — | 从上表选择一个资源。 |
| `access_key` | String | 是 | — | Datadog API 密钥。 |
| `secret_key` | String | 是 | — | 具备资源读取权限的 Datadog 应用密钥。 |
| `site` | String | 否 | `datadoghq.com` | 也可选 `us3.datadoghq.com`、`us5.datadoghq.com`、`datadoghq.eu`、`ap1.datadoghq.com`、`ap2.datadoghq.com`、`uk1.datadoghq.com`、`ddog-gov.com` 和 `us2.ddog-gov.com`。 |
| `start_time` | String | 日志和审计日志必填 | — | 含起点的 ISO-8601 时间。 |
| `end_time` | String | 否 | 作业初次启动时间 | 已排除结束时刻的 ISO-8601 时间。空值使用默认值，仅适用于日志和审计日志。 |
| `logs_query` | String | 否 | 空 | Datadog 日志搜索表达式，最多 4,096 个字符，仅适用于日志。 |
| `slice_days` | Integer | 否 | `1` | 每个事件时间窗口的天数，范围 1 到 31。整个采集区间最多 36,525 天。 |
| `metrics_window_seconds` | Integer | 否 | `5184000` | 指标目录按上报时间回溯的秒数，范围 0 到 5,184,000。0 使用服务端不限上报时间的目录查询，包含已停止上报的指标。 |
| `page_size` | Integer | 否 | `100` | 每页请求条数，范围 1 到 1,000，并受各接口上限约束。停机计划和监控器 ID 偏移接口无此参数。 |
| `request_timeout_ms` | Integer | 否 | `10000` | 连接和套接字空闲超时，范围 100 到 60,000 毫秒。 |
| `page_deadline_ms` | Integer | 否 | `120000` | 单页总时限，包含重试等待，范围 100 到 300,000 毫秒。 |
| `max_attempts` | Integer | 否 | `4` | 单页读取的最大尝试次数，范围 1 到 8。 |
| `max_response_bytes` | Integer | 否 | `8388608` | 单个响应解压后的最大字节数，范围 1,024 到 16,777,216。 |

## 输出与恢复

四个输出列均为字符串。`resource` 是资源名称。`id` 是服务端标识符、合成测试公开 ID，或标量指标名称。
`timestamp` 保存日志和审计日志中的原始时间文本。这两列允许为空。`record` 是服务端单条记录的 JSON，
标量指标名称保留为 JSON 字符串。JSON 空白及数字表示可被规范化，嵌套值、数值和 Unicode 得以保留。

每个事件窗口包含起点并排除终点。起止时间相同时返回空结果，时间戳相同的不同事件会分别输出。
首次启动固定结束时间，恢复沿用检查点保存的页和窗口。凭据和决定查询范围的参数须与检查点一致。
独立提交的新作业按配置区间读取，各作业之间没有外部水位。若定时采集需要覆盖晚到的数据，可配置重叠区间并使用幂等下游。

每个配置段只运行一个活跃读取器。作业恢复可能重放检查点之后已输出的数据。Datadog 分页反映实时数据，
数据变化、保留期和游标有效期会影响扫描。游标过期、已提交的页前缀发生变化、响应不完整或权限不足时，作业失败。
单个响应和资源或事件窗口的总页数都设有上限。收到 HTTP 429 时，作业在单页总时限内按服务端提示等待重试。

## 示例

```hocon
env {
  job.mode = "BATCH"
  checkpoint.interval = 1000
}
source {
  Datadog {
    resource = "logs"
    site = "datadoghq.com"
    access_key = ${DATADOG_ACCESS_KEY}
    secret_key = ${DATADOG_SECRET_KEY}
    start_time = "2026-01-01T00:00:00Z"
    end_time = "2026-01-02T00:00:00Z"
    logs_query = "service:checkout"
  }
}
sink {
  Console {}
}
```

[Datadog 站点](https://docs.datadoghq.com/getting_started/site/) ·
[Datadog API](https://docs.datadoghq.com/api/latest/)

## 变更日志

<ChangeLog />
