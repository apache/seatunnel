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

import ChangeLog from "../changelog/connector-http-campaign-monitor.md";

# Campaign Monitor

## 描述

Campaign Monitor Source 读取 `https://api.createsend.com` 上 v3.3 API 的十二种资源。使用有权读取所选资源的账户 API 密钥。连接器通过 HTTP Basic 认证发送该密钥，并从账户可见的客户端列表发现父资源。子记录始终保留所属客户端。[API 认证](https://www.campaignmonitor.com/api/v3-3/getting-started/)、[账户接口](https://www.campaignmonitor.com/api/v3-3/account/)。

| 资源 | 主键 | 返回形式 |
| --- | --- | --- |
| `clients` | `ClientID` | 账户列表 |
| `admins` | `EmailAddress` | 账户列表 |
| `client_details` | `BasicDetails.ClientID` | 每客户端一个对象 |
| `segments` | `SegmentID` | 客户端列表 |
| `templates` | `TemplateID` | 客户端列表 |
| `people` | `EmailAddress` | 客户端列表 |
| `tags` | `Name` | 客户端列表 |
| `subscriber_lists` | `ListID` | 客户端列表 |
| `suppression_lists` | `EmailAddress` | 客户端分页 |
| `sent_campaigns` | `CampaignID` | 客户端分页 |
| `draft_campaigns` | `CampaignID` | 客户端列表 |
| `scheduled_campaigns` | `CampaignID` | 客户端列表 |

选择任一客户端子资源会自动发现父客户端，选择 `clients` 后这些父记录也会输出。读取 `client_details` 时会校验返回的客户端标识，并从结果中移除 API 密钥字段。预览、归档或内容地址作为普通值保留。[客户端接口](https://www.campaignmonitor.com/api/v3-3/clients/)。

## 选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| token | string | 是 | | Campaign Monitor 账户 API 密钥。 |
| resources | list | 否 | [] | 上表中的资源名，空列表读取全部十二种资源。 |
| start_date | string | 读取已发送活动时必填 | | 包含该发送日期，格式为 `YYYY-MM-DD`。 |
| end_date | string | 否 | | 排除该发送日期，格式为 `YYYY-MM-DD`，须晚于 `start_date`。 |
| page_size | int | 否 | 1000 | 已发送活动和禁止发送名单的页大小，范围 10 至 1,000。 |
| max_pages_per_collection | int | 否 | 10000 | 单个客户端的单种集合最多接受的页数。 |
| max_pages_per_scan | int | 否 | 100000 | 一次扫描最多接受的响应数。 |
| max_unpaginated_items | int | 否 | 10000 | 单次完整列表响应里的最大记录数。 |
| tls_trust_certificate | string | 否 | JVM 默认信任配置 | 仅此 Source 使用的 PEM 或 Base64 DER X.509 信任锚，仍校验主机名。 |
| connect_timeout_ms | int | 否 | 10000 | TCP 连接超时，单位为毫秒。 |
| request_timeout_ms | int | 否 | 30000 | 单次 HTTPS 尝试截止时间，单位为毫秒。 |
| operation_timeout_ms | int | 否 | 120000 | 一次逻辑读取包含重试的总时限，不小于 `request_timeout_ms`。 |
| request_interval_ms | int | 否 | 100 | 相邻尝试的最短时间，单位为毫秒。 |
| max_retries | int | 否 | 3 | 暂时性读取失败时的额外尝试次数。 |
| max_retry_delay_ms | int | 否 | 60000 | 最大允许服务端限流提示或重试等待。 |
| max_response_bytes | int | 否 | 8388608 | gzip 解压前和解压后各自的响应字节上限。 |

## 日期、分页与恢复

已发送活动使用 API 的日期参数 `sentFromDate` 和 `sentToDate`。服务返回的 `SentDate` 是没有时区偏移的日期时间文本。连接器按日期部分应用筛选并保留原值。若需在另一个批次回读交叠的发送记录，可将先前的发送日期用作新下界。不同批次的行可用资源名、客户端和记录标识区分。其他资源各自保留服务返回的日期字段。[已发送活动](https://www.campaignmonitor.com/api/v3-3/clients/)。

两个分页集合使用 v3.3 的 `Results` 和页元数据。若还有下一页，短页和空页都会继续。元数据矛盾、非法日期、认证错误或重定向会导致读取失败。暂时性故障可在给定次数和总时限内重试，服务提示的等待超过配置上限时会报错。

恢复时先复核已保存的页面摘要和客户端，再从对应行继续读取。检查点不保存 API 密钥或原始记录。已完成的集合保持完成。密钥过期、日期范围变更或重放页面变化可能需要重启一次新批次，集合也可能在分页期间变化。

## 输出

五列均为字符串。

| 列名 | 含义 |
| --- | --- |
| `resource` | 所选的资源名。 |
| `id` | 服务原生记录标识。 |
| `client_id` | 客户端及子集合的客户端标识，账户管理员的值为 null。 |
| `sent_date` | 已发送活动的原始 `SentDate` 文本，其他资源为 null。 |
| `record` | 保留嵌套结构、null 与精确数值的原生 JSON。 |

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  CampaignMonitor {
    token = ${CAMPAIGN_MONITOR_TOKEN}
    resources = ["clients", "subscriber_lists", "sent_campaigns"]
    start_date = "2026-01-01"
    end_date = "2026-02-01"
  }
}
```

## 变更日志

<ChangeLog />
