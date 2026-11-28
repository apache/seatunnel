import ChangeLog from '../changelog/connector-http-adjust.md';

# Adjust

{/*
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
*/}

`Adjust` Source 通过 Adjust Report Service JSON API 读取有界日报。请安装 `connector-http-adjust`，并使用有权访问目标报表的 Adjust API 令牌。 [Adjust JSON 报表](https://dev.adjust.com/en/api/rs-api/reports) · [身份认证](https://dev.adjust.com/en/api/rs-api/authentication)

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 | | 用于 Bearer 认证的 Adjust API 令牌。 |
| `start_date` | String | 是 | | 首个报表日期，包含该日，格式为 `YYYY-MM-DD`。 |
| `dimensions` | `List<String>` | 是 | | 有序的维度标识列表，例如 `app` 和 `country`。如果为空，则仅按必需的 `day` 分组。 |
| `metrics` | `List<String>` | 是 | | 非空的有序指标标识列表，例如 `installs` 或 `network_cost`。 |
| `additional_metrics` | `List<String>` | 否 | `[]` | 额外的指标标识，例如队列或应用专用指标。 |
| `metric_scales` | `Map<String, String>` | 否 | `{}` | 按指标设置小数位数，范围为 0 到 38。未配置的指标使用 18 位。 |
| `until_today` | Boolean | 否 | false | 包含当天。默认读取到昨天。 |
| `utc_offset` | String | 否 | `+00:00` | 固定报表时区偏移，格式为 `+HH:MM` 或 `-HH:MM`，距 UTC 不超过 14 小时。 |
| `base_url` | String | 否 | `https://automate.adjust.com` | 受信任的 API 源地址，允许末尾带斜杠。 |
| `allow_insecure_http` | Boolean | 否 | false | 对显式信任的隔离端点启用明文 HTTP。 |
| `request_timeout_ms` | Integer | 否 | 30000 | 单日报表请求总时限，含尝试及重试等待，范围为 1 到 300000 毫秒。 |
| `max_attempts` | Integer | 否 | 4 | 单日报表的最大 HTTP 尝试次数，范围为 1 到 8。 |
| common-options | | 否 | | [Source 通用参数](../common-options/source-common-options.md)。 |

## 动态 Schema

`adjust_report` 首列为非空的 `day DATE`，其后依次为所选的可空 `STRING` 维度、可空 `DECIMAL(38, scale)` 指标和额外指标。即使 `dimensions` 中指定了 `day`，也只输出一次。其余列名必须唯一。请使用 Adjust 报表 API 或元数据接口提供的维度和指标标识。 [筛选数据](https://dev.adjust.com/en/api/rs-api/filters-data) · [事件标识](https://dev.adjust.com/en/api/rs-api/events)

JSON 数值和数字字符串会精确转换。无效数值或超出字段范围时该日读取失败，不进行舍入。小数位数设为 0 时最多可保存 38 位整数，适用于计数指标。维度和指标缺失或为 null 时输出 null。连接器只输出 `rows` 内的数据，合计和辅助字段不进入结果表。

## 日期、限制与恢复

每天发起一个请求，使用固定时区偏移和 ISO 日期格式。作业开始扫描时确定最后一天。启用 `until_today` 后，当天的数据仍可能由服务端更新。默认使用 UTC，也可指定固定偏移。命名时区和夏令时切换不在固定偏移语义内。

HTTP 204 或空 `rows` 会推进日期，并输出零条记录。若响应包含非空分页、警告、重复维度组合或其他日期的记录，该日读取失败且不输出该日记录。JSON 报表接口未定义续页请求。每个作业最多读取 3660 天，输出最多 96 列，每天最多 20000 条记录。响应压缩前后均不得超过 8 MiB。

源端并行度设为 1。检查点保存原始截止日期与下一个未完成的报表日期。恢复时需要保持相同报表参数，可以使用更新后的令牌。检查点不含令牌或报表记录。服务端结果可能在两次读取间变化，最近一次完成检查点之后输出的数据在恢复时可能被再次读取。

HTTPS 默认验证证书及主机名。遇到重定向、身份认证或权限错误时终止请求。HTTP 429、500、502、503 和 504 可以在总时限与尝试次数内重试。服务端 `Retry-After` 指定的最短等待时间也必须落在该日总时限内。 [Adjust 限流](https://dev.adjust.com/en/api/rs-api/rate-limits)

## 示例

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}

source {
  Adjust {
    token = ${?ADJUST_TOKEN}
    start_date = "2026-10-01"
    dimensions = ["app", "country"]
    metrics = ["installs", "network_cost"]
    additional_metrics = ["purchase_events"]
    metric_scales = { installs = "0", network_cost = "12", purchase_events = "0" }
    until_today = false
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
