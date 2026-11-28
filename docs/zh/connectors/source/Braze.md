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

import ChangeLog from '../changelog/connector-http-braze.md';

# Braze

安装 `connector-http-braze` 后，Braze Source 从一个 Braze 工作区读取有界 JSON 导出数据。每个 source 配置块选择一个资源，源并行度设为 `1`。工作区 REST API 密钥需要具备所选接口的权限。读取子资源时，还要具备父资源列表权限。

## 资源

| `resource` | Braze GET 路径 | 输出 |
| --- | --- | --- |
| `campaigns` | `/campaigns/list` | 每行一个营销活动对象 |
| `campaigns_analytics` | `/campaigns/data_series` | 营销活动每日指标，包含消息变体数据 |
| `campaigns_details` | `/campaigns/details` | 每个已发现营销活动的详情 |
| `canvases` | `/canvas/list` | 每行一个 Canvas 对象 |
| `canvases_analytics` | `/canvas/data_series` | Canvas 每日汇总及可用的步骤和变体细分指标 |
| `canvases_details` | `/canvas/details` | 每个已发现 Canvas 的详情 |
| `events` | `/events/list` | 每个事件名称一个 `{"event_name":"…"}` 对象 |
| `events_analytics` | `/events/data_series` | 每个已发现事件每天的发生次数 |
| `kpi_daily_new_users` | `/kpi/new_users/data_series` | 工作区每日新增用户数 |
| `kpi_daily_active_users` | `/kpi/dau/data_series` | 工作区每日活跃用户数 |
| `kpi_daily_app_uninstalls` | `/kpi/uninstalls/data_series` | 工作区每日应用卸载次数 |
| `cards` | `/feed/list` | 旧版 News Feed 卡片对象 |
| `cards_analytics` | `/feed/data_series` | 旧版 News Feed 每日互动指标 |
| `segments` | `/segments/list` | 每行一个用户分组对象 |
| `segments_analytics` | `/segments/data_series` | 用户分组的每日规模估算 |
| `segments_details` | `/segments/details` | 每个已发现用户分组的详情 |

两个旧版 News Feed 资源要求工作区仍可访问对应的历史接口。当前 Braze 导出文档已不列出这两个接口。如果服务返回 HTTP 404 或 410，任务会明确报告旧版接口不可用。Content Cards 是独立资源。[当前 Braze 导出文档](https://www.braze.com/docs/api/endpoints/export) · [News Feed 历史接口](https://www.postman.com/braze-inc/braze-public-workspace/request/mqcth3a/news-feed-card-analytics)

## 配置

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `instance` | string | 必填 | 控制台实例，如 `us-01` 或 `eu-01`。支持 `us-01` 至 `us-08`、`us-10`、`eu-01`、`eu-02`、`au-01`、`id-01`、`jp-01` 和 `kr-01`，大小写均可。 |
| `token` | string | 必填 | 工作区 REST API 密钥，通过 HTTPS Bearer 认证发往所选实例。 |
| `resource` | string | 必填 | 资源表中的一个导出名称。 |
| `start_date` | string | 无 | 格式为 `YYYY-MM-DD`。分析和 KPI 的起始日，包含当天，必填。直接读取 `campaigns`、`canvases` 或 `cards` 列表时，可以按最后编辑时间筛选，只返回晚于该日 UTC 零点的记录。 |
| `end_date` | string | 当前 UTC 日期 | 格式为 `YYYY-MM-DD`，仅用于分析及 KPI，包含最后一天。若结束日期为当前 UTC 日期，窗口截至读取开始时刻。 |
| `max_pages` | int | `2000` | 编号分页的最大页数，包含父资源发现及确认结束的空页。范围 1–20000。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接超时，单位毫秒。范围 1–120000。 |
| `read_timeout_ms` | int | `30000` | 套接字读取超时，单位毫秒。范围 1–120000。 |
| `request_deadline_ms` | int | `60000` | 每次请求的总时限，涵盖连接、TLS、读取和重试等待，单位毫秒。范围 1–300000。 |
| `max_attempts` | int | `4` | 可重试状态码的最大请求次数，含第一次。范围 1–10。 |
| `max_response_bytes` | int | `8388608` | 响应压缩前后各自允许的最大字节数。范围 1024–33554432。 |

分析区间须从 2000-01-01 或其后开始，两端都不得晚于当前 UTC 日期。窗口首尾日期均包含，Canvas 分析每窗最多 14 天，其余分析及 KPI 每窗最多 100 天。发现父资源时遍历完整列表，供应商的默认归档可见性继续生效。营销活动、Canvas 和用户分组每页最多 100 条，事件名称每页最多 250 条，旧版 News Feed 遇到首个空页才停止。[Braze REST 实例及权限](https://www.braze.com/docs/api/basics)

## 输出与恢复

三列均为字符串类型。

| 字段 | 值 |
| --- | --- |
| `entity_id` | 列表对象 ID、事件名称，或详情及分析对应的父资源 ID。工作区 KPI 为 null。 |
| `time` | 分析或 KPI 数据中供应商返回的 ISO-8601 时间。列表和详情为 null。 |
| `data` | 完整 JSON 记录，包含空值、嵌套字段和数值。子资源带有对应的 `campaign_id`、`canvas_id`、`event_name`、`card_id` 或 `segment_id`。 |

检查点保存首次确定的分析截止时间以及页号、父项和记录偏移量。恢复时会重新读取所需页面，核对当前父项 ID 的顺序和已部分输出的记录。保存位置上的数据发生变化时，任务会明确失败。Braze 列表和指标在读取过程中可能变化，接口未提供不可变快照。需要开启新一轮抽取时，可配置新的日期范围。

连接器验证 HTTPS 证书和主机名，禁用重定向，并要求 HTTP 200 响应携带 Braze 成功标记。HTTP 408、429 和 5xx 按剩余时限重试，限流等待遵循 Braze 响应头给出的延迟或重置时间。[Braze 限流说明](https://www.braze.com/docs/api/api_limits)

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Braze {
    instance = "us-01"
    token = ${BRAZE_API_KEY}
    resource = "canvases_analytics"
    start_date = "2024-01-01"
    end_date = "2024-01-16"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
