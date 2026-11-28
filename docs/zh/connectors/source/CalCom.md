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

import ChangeLog from "../changelog/connector-http-cal-com.md";

# Cal.com

## 描述

Cal.com Source 在有界批作业中从 `https://api.cal.com` 读取六种资源。使用 Cal.com 的个人 API 密钥，可见数据取决于该密钥及所属账户。[Cal.com API v2](https://cal.com/docs/api-reference/v2/introduction)。

| 资源 | 接口 | 记录主键 | 读取方式 |
| --- | --- | --- | --- |
| `event_types` | `/v2/event-types` | `id` | 默认按组返回，当前版本为扁平列表。 |
| `my_profile` | `/v2/me` | `id` | 一个用户资料对象。 |
| `schedules` | `/v2/schedules` | `id` | `2024-06-11` 版本返回完整列表。 |
| `calendars` | `/v2/calendars` | `externalId` | 每个已连接集成下的日历。 |
| `bookings` | `/v2/bookings` | `id` | 原样使用游标，或校验旧版偏移量分页。 |
| `conferencing` | `/v2/conferencing` | `id` | 已安装的会议应用元数据。 |

事件类型默认使用 `2024-04-15` 分组版本，选择 `2026-06-12` 可读取当前扁平版本。日历保留所属集成的上下文。会议应用保留接口提供的公开元数据，丢弃意外返回的密钥或令牌字段。[事件类型](https://cal.com/docs/api-reference/v2/event-types/list-event-types)、[日历](https://cal.com/docs/api-reference/v2/calendars/get-all-calendars)、[会议应用](https://cal.com/docs/api-reference/v2/conferencing/list-your-conferencing-applications)。

## 选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| token | string | 是 | | 以 `cal_` 开头的个人 API 密钥。 |
| resources | list | 否 | [] | 上表中的资源名，空列表读取全部六种资源。 |
| event_types_api_version | string | 否 | 2024-04-15 | 分组版 `2024-04-15` 或扁平版 `2026-06-12`。 |
| bookings_api_version | string | 否 | 2026-05-01 | 游标版 `2026-05-01` 或偏移量版 `2024-08-13`。 |
| bookings_page_size | int | 否 | 100 | 每页请求的预订数，范围 1 至 100。 |
| max_items_per_response | int | 否 | 10000 | 单次完整列表的最大记录数，嵌套列表按展开后的数量限制。 |
| max_pages_per_resource | int | 否 | 10000 | 单种资源最多读取的页数。 |
| tls_trust_certificate | string | 否 | JVM 默认信任配置 | 仅此 Source 使用的可选 PEM 或 Base64 DER X.509 信任锚，仍校验主机名。 |
| connect_timeout_ms | int | 否 | 10000 | TCP 连接超时，单位为毫秒。 |
| request_timeout_ms | int | 否 | 30000 | 单次 HTTPS 尝试的时限，单位为毫秒。 |
| operation_timeout_ms | int | 否 | 60000 | 一次逻辑读取的总时限，包含重试和等待，不小于 `request_timeout_ms`。 |
| request_interval_ms | int | 否 | 600 | 相邻尝试的最短间隔，单位为毫秒。 |
| max_retries | int | 否 | 3 | 暂时性读取失败后的额外尝试次数。 |
| max_retry_delay_ms | int | 否 | 30000 | 最大 Retry-After 或退避等待时间。 |
| max_response_bytes | int | 否 | 8388608 | 解压前和解压后各自的响应字节上限。 |

## 分页与检查点

预订 `2026-05-01` 返回 `pagination.hasMore` 和 `pagination.nextCursor`。Source 将游标作为不透明值放入查询参数，只要服务指示有下一页就会继续，空页和短页同样适用。预订 `2024-08-13` 使用 `skip` 与 `take`，Source 验证返回的偏移量分页元数据，再按请求页大小推进。其余资源按各接口的完整响应读取。[预订接口](https://cal.com/docs/api-reference/v2/bookings/get-all-bookings)、[日程接口](https://cal.com/docs/api-reference/v2/schedules/get-all-schedules)。

检查点保存资源位置、当前页及已发出的行数。恢复时先校验当前页摘要，再从下一行继续。若页面已变化、游标过期，或更改了 API 密钥、版本和资源范围，可能需要新启批作业。服务的数据可以在扫描期间变化，新批次读取密钥当前可见的数据。

认证失败、重定向、返回结构或分页不符合接口约定，以及 HTTP 200 内的 API 错误都会明确报错。暂时性错误可在配置的时限内重试。

## 输出

四列均使用字符串类型。

| 列名 | 含义 |
| --- | --- |
| `resource` | 所选资源名。 |
| `id` | 原始记录标识，数字主键以字符串表示。 |
| `parent` | 事件类型分组或日历连接的 JSON 元数据，省略子记录数组。其他记录为 null。 |
| `record` | JSON 记录，保留原字段名、嵌套数据、数值精度和 null。 |

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  CalCom {
    token = ${CAL_COM_TOKEN}
    resources = ["event_types", "calendars", "bookings"]
  }
}
```

## 变更日志

<ChangeLog />
