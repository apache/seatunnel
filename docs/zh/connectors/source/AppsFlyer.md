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

import ChangeLog from '../changelog/connector-http-appsflyer.md';

# AppsFlyer

在 SeaTunnel Zeta 批作业中读取 AppsFlyer v5 Pull API CSV 报表，Source 并行度为 1。
使用 `bin/install-plugin.sh` 安装 `connector-http-appsflyer`。准备有目标应用及报表访问权限的 V2
Pull API 令牌。参阅[原始报表](https://support.appsflyer.com/hc/en-us/articles/360007530258-Pull-API-raw-data)
和[聚合报表](https://support.appsflyer.com/hc/en-us/articles/207034346-Pull-API-aggregate-data)。

## 资源

| 资源 | v5 报表 | Source 游标 |
| --- | --- | --- |
| `in_app_events` | `in_app_events_report` | `event_time` |
| `organic_in_app_events` | `organic_in_app_events_report` | `event_time` |
| `retargeting_in_app_events` | `in-app-events-retarget` | `event_time` |
| `installs` | `installs_report` | `install_time` |
| `organic_installs` | `organic_installs_report` | `install_time` |
| `retargeting_installs` | `installs-retarget` | `install_time` |
| `uninstall_events` | `uninstall_events_report` | `event_time` |
| `organic_uninstall_events` | `organic_uninstall_events_report` | `event_time` |
| `daily_report` | `daily_report` | `date` |
| `retargeting_daily_report` | `daily_report` + `reattr=true` | `date` |
| `partners_report` | `partners_by_date_report` | `date` |
| `retargeting_partners_report` | `partners_by_date_report` + `reattr=true` | `date` |
| `partners_events_report` | `partners_by_date_report` | `date` |
| `retargeting_partners_events_report` | `partners_by_date_report` + `reattr=true` | `date` |
| `geo_report` | `geo_by_date_report` | `date` |
| `retargeting_geo_report` | `geo_by_date_report` + `reattr=true` | `date` |
| `geo_events_report` | `geo_by_date_report` | `date` |
| `retargeting_geo_events_report` | `geo_by_date_report` + `reattr=true` | `date` |

合作伙伴和地区事件视图按报表中的事件指标列展开，每条 CSV 记录中的每个事件名输出一行。同时选择汇总视图和
对应事件视图时共用一次下载。拉新和重定向报表分别请求。

## 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `token` | string | 必填 | V2 Bearer 令牌，请在作业文件外存储。 |
| `app_id` | string | 必填 | AppsFlyer 应用 ID，填写未经 URL 编码的单个路径段。 |
| `start_date` | string | 必填 | 配置时区内的起始时间，包含边界。 |
| `end_date` | string | 可选 | 结束时间，不包含边界。未配置时，原始报表截至初次启动的整秒时间，聚合报表包含初次启动时应用所在时区的当天。 |
| `timezone` | string | `UTC` | 与 AppsFlyer 应用配置一致的 IANA 时区。 |
| `resources` | list of strings | 全部 18 个 | 资源表中互不重复的一个或多个名称。 |
| `api_url` | string | `https://hq1.appsflyer.com` | AppsFlyer HTTPS 源站地址，可指定端口。 |
| `download_origins` | list of strings | `[]` | 最多八个可受委托下载报表的 HTTPS 源站。填写当前环境中受信任的准确域名及端口。 |
| `max_window_records` | int | `100000` | 输入行数达到阈值或展开的输出行数超过阈值时拆分请求。范围 2–1000000，仍受供应商上限约束。 |
| `max_response_bytes` | int | `33554432` | 对原始传输和解压后的报表字节数分别限制。范围 1024–134217728。 |
| `max_buffer_bytes` | int | `33554432` | 每个响应暂存的 JSON 负载 UTF-8 字节上限。范围 1024–134217728。 |
| `max_field_chars` | int | `262144` | 每个 CSV 字段的 UTF-16 单元上限。范围 16–1048576。 |
| `max_requests` | int | `1000` | 每次读取器运行时允许的 HTTP 请求总数，包含窗口、重定向和重试。范围 1–100000。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接超时。范围 1–300000。 |
| `read_timeout_ms` | int | `15000` | TLS 握手和响应读取的空闲超时。范围 1–300000。 |
| `request_timeout_ms` | int | `120000` | 窗口的总请求时限，包含重定向、读取响应和重试。范围 1–300000。 |
| `max_attempts` | int | `3` | 每个窗口的初次和重试次数之和。范围 1–10。 |

## 时间范围、限额和恢复

只要选择聚合视图，日期参数就必须使用 `yyyy-MM-dd`。只读取原始报表时还可使用秒精度、带偏移的
ISO 时间戳，或明确对应唯一时间点的应用本地时间，例如 `2026-11-01 03:00:00`。
请求聚合报表时将配置的结束日期换算为包含当天的 API 日期。原始报表使用不含结束边界的时间戳和应用
设置中的币种。请将时区设成应用的时区，确保供应商按同一时区解释日期。

连接器遵循原始报表 90 天和聚合报表 25 个月的标准保留期。具体可查询时间和数据刷新取决于账号及报表。
报表达到行数或字节上限时会尝试拆成互不重叠的区间。原始报表可按秒拆分，聚合报表按整天拆分。
最小区间仍超限时，作业失败且该区间不会输出不完整报表。重试和拆分会消耗请求预算，也可能消耗服务商限额。
在请求时限内遵循 `Retry-After` 处理限流。每日限额用完时直接失败。参阅[报表保留期](https://support.appsflyer.com/hc/en-us/articles/4415473374865-Data-availability-windows)
和[报表限额](https://support.appsflyer.com/hc/en-us/articles/207034366-Report-generation-quotas-rate-limitations)。

HTTPS 使用 JVM 信任库和域名校验。报表下载最多允许三次重定向，委托下载的源站要在
`download_origins` 中。Bearer 令牌只发送给 API 源站，跨源后即使重定向回 API 源站也不再附带令牌。
签名下载地址只在内存中使用。

Zeta 检查点保存剩余时间窗口和当前报表的行位置。恢复会跳过完成的窗口，继续沿用首次运行确定的结束时间和
已拆分的窗口。如果检查点前已输出未完成报表中的部分行，恢复时将先比较重取报表的记录内容和顺序摘要，
一致后才继续输出。上游报表可能变化或过期，且没有提供快照令牌。下游提交取决于 Sink，
再次启动一个全新作业时可能会看到供应商更新的数据。

## 输出

每行包含五个字符串字段 `resource`、`app_id`、`report_time`、`time_zone` 和 `data`。
`data` 是序列化为字符串的 JSON 对象。CSV 列名转换为键，冲突列以原名放在 `additional_columns`。
内容为空、`N/A`、`NULL`、带逗号或嵌入换行的原始文本都会保留。已识别的原始布尔值、时间戳和
聚合数字按相应类型输出，缺失的有类型值输出 JSON null。原始报表的金额保留精确文本，已知聚合数值保留
十进制精度。百分比保留报表数值单位，例如 `12.25%` 输出为 `12.25`。带偏移的时间统一转成 UTC，
没有偏移的保留为本地时间，`time_zone` 指明请求的时区。事件视图增加 `event_name`、
`event_unique_users`、`event_counter` 和 `event_sales`，缺失的事件指标输出 null。

## 示例

使用当前保留期内的报表日期，通过受保护的配置变量提供 `APPSFLYER_TOKEN`。
若报表委托给另一个源站下载，请配置受信任的 `download_origins`。

```hocon
env {
    job.mode = "BATCH"
    parallelism = 1
}
source {
    AppsFlyer {
        token = ${APPSFLYER_TOKEN}
        app_id = "com.example.app"
        start_date = "2026-11-01"
        end_date = "2026-11-03"
        timezone = "America/New_York"
        resources = ["in_app_events", "organic_installs"]
    }
}
sink {
    Console {}
}
```

## 更新日志

<ChangeLog />
