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

import ChangeLog from '../changelog/connector-http-acuity-scheduling.md';

# Acuity Scheduling

`AcuityScheduling` Source 通过有界作业读取 Acuity Scheduling API v1 的七个资源。请将 Source 并行度设为 1。端到端测试使用 SeaTunnel Zeta。

从 Acuity Integrations 获取数字用户 ID 和 API Key。连接器使用 HTTPS Basic 认证，并通过 Worker JVM 的证书信任库验证服务器证书及主机名。[Acuity 认证说明](https://developers.acuityscheduling.com/reference/quick-start)

## Source 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `api_url` | String | `https://acuityscheduling.com` | 受信任的 HTTPS 服务地址。连接器追加固定 API 路径。 |
| `username` | String |  | 必填。Acuity 数字用户 ID。 |
| `password` | String |  | 必填。通过部署环境的密钥机制提供 Acuity API Key。 |
| `resources` | List&lt;String&gt; | 全部七个 | 按读取顺序列出资源，名称必须互不重复。 |
| `start_date` | String |  | 包含该日期。读取预约或日程阻塞时必填。 |
| `end_date` | String | 当前日期 | 包含该日期。默认在作业启动时按 timezone 计算一次，恢复时沿用。 |
| `resume_date` | String |  | 可选。上次成功的日期边界。配置后重读之前的回看区间。 |
| `timezone` | String | `UTC` | 用于计算默认 end_date 的 IANA 企业或日历时区。此参数不会变更服务端时区。 |
| `lookback_days` | Integer | `7` | 从 resume_date 向前回看且限制在 start_date 之后的天数，范围 0–90。 |
| `window_days` | Integer | `31` | 最初每个日期窗口最多包含的天数，范围 1–365。 |
| `max_date_results` | Integer | `100` | 每次预约或日程阻塞查询的 max 值，范围 1–100。 |
| `calendar_id` | Integer |  | 可选正整数。仅对预约和日程阻塞过滤日历 ID。 |
| `max_requests` | Integer | `10000` | 一次读取最多执行的逻辑请求数，包括窗口探测，范围 1–50,000。 |
| `max_response_bytes` | Integer | `4194304` | 分别限制网络响应和解压后内容的字节数，范围 1,024–16,777,216。 |
| `max_records_per_response` | Integer | `20000` | 单个非日期资源列表的记录上限，范围 1–50,000。 |
| `connect_timeout_ms` | Integer | `10000` | TCP 连接超时毫秒数，范围 1–300,000。 |
| `read_timeout_ms` | Integer | `10000` | TLS 握手和读取响应的空闲超时，范围 1–300,000 毫秒。 |
| `request_timeout_ms` | Integer | `30000` | 响应的总时限，包含重试和等待，范围 1–300,000 毫秒。 |
| `max_attempts` | Integer | `3` | 每次请求最多尝试次数，包含首次请求，范围 1–10。 |

日期参数使用 `YYYY-MM-DD`。使用默认结束日期时，请将 `timezone` 设置为业务或所选日历的时区。

## 资源

| 资源 | 读取方式 |
| --- | --- |
| [`appointments`](https://developers.acuityscheduling.com/reference/get-appointments) | 按日期读取，包含已取消的预约和内嵌的登记表单答复。 |
| [`calendars`](https://developers.acuityscheduling.com/reference/get-calendars) | 全量读取有权限访问的日历。 |
| [`clients`](https://developers.acuityscheduling.com/reference/clients) | 全量读取客户列表。 |
| [`appointment-types`](https://developers.acuityscheduling.com/reference/appointment-types) | 全量读取预约类型。 |
| [`blocks`](https://developers.acuityscheduling.com/reference/blocks) | 按日期读取日历阻塞时段。 |
| [`labels`](https://developers.acuityscheduling.com/reference/labels) | 全量读取标签。 |
| [`forms`](https://developers.acuityscheduling.com/reference/forms) | 全量读取登记表单定义及字段。 |

每行有五个 STRING 列。`resource` 和 `record_id` 标识对象。`data` 保留完整的服务端 JSON，包括嵌套表单、数值及 null。客户对象没有 ID 时，使用服务端邮箱的哈希作为 `record_id`，没有邮箱时使用对象哈希。可空的 `window_start` 和 `window_end` 表示预约和日程阻塞的查询日期窗口，包含两端。负载中可能包含联系方式及表单答复。

## 日期窗口与恢复

预约和日程阻塞通过 `minDate`、`maxDate` 按排定的日历日期过滤。窗口描述服务端查询范围，并不表示记录的创建或更新时间。日期按日历日计算，可跨越夏令时切换。配置 `resume_date` 后，从 `start_date` 与 `resume_date - lookback_days` 中较晚的日期开始重读。回看区间之外的记录仍可能变更或重新安排。不同作业读取的日期窗口有重叠时，可按资源与记录标识去重。

响应达到请求上限时，连接器继续将窗口拆分，并丢弃探测响应。单日仍达到上限时，作业报出日期和端点并失败，避免静默截断。只读取一个日历时可指定 `calendar_id`。其余五个资源整表读取并限制响应大小。

检查点保存已确定的截止日期、待查询窗口及资源进度。恢复时需保持账号、端点、资源列表和日期配置一致。服务端数据可能在不同请求之间变化，恢复也可能重放最后一个完成的检查点之后的记录。

HTTP 429 和部分 5xx 在次数与总时限内可重试。连接器遵循有效的 `Retry-After`，同时限制压缩内容和解压后的字节数。出现重定向时请求失败。错误诊断不包含认证值或响应详情。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  AcuityScheduling {
    username = ${ACUITY_USER_ID}
    password = ${ACUITY_API_KEY}
    resources = ["appointments", "blocks", "forms"]
    start_date = "2025-01-01"
    end_date = "2025-03-31"
    timezone = "America/New_York"
  }
}

sink {
  Console {}
}
```

<ChangeLog />
