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

# BugSnag Source

`BugSnag` 从 v2 Data Access API 有界读取数据，源端并行度为 1。发行包中缺少此连接器时，请安装 `connector-http-bugsnag`。

在 **My account → Personal auth tokens** 创建个人访问令牌。连接器在每次请求中发送 `Authorization: token <token>` 和 `X-Version: 2`。访问权限取决于令牌所属用户的组织、项目权限及 BugSnag 套餐。 [Data Access API](https://developer.smartbear.com/bugsnag/docs/bugsnag-data-access-api) · [认证](https://developer.smartbear.com/bugsnag/docs/data-access-authentication)

## 资源

| 资源 | API 路径 | 输出时间字段 |
| --- | --- | --- |
| `organizations` | `/user/organizations` | |
| `projects` | `/organizations/{organization_id}/projects` | `created_at` |
| `saved_searches` | `/projects/{project_id}/saved_searches` | |
| `saved_searches_usage_summary` | `/saved_searches/{search_id}/usage_summary` | |
| `errors` | `/projects/{project_id}/errors` | `last_seen` |
| `events` | `/projects/{project_id}/events` | `received_at` |
| `pivots` | `/projects/{project_id}/pivots` | |
| `supported_integrations` | `/integrations` | |
| `collaborators` | `/organizations/{organization_id}/collaborators` | |
| `teams` | `/organizations/{organization_id}/teams` | |
| `event_fields` | `/projects/{project_id}/event_fields` | |
| `releases` | `/projects/{project_id}/releases` | `release_time` |
| `trace_fields` | `/projects/{project_id}/trace_fields` | |

连接器会查询子资源所需的组织、项目与已保存搜索，即使父行未被选中或超出配置的时间范围。事件以查询参数 `full_reports=true` 请求完整报告。每个已保存搜索的用量摘要是一个对象，保留真实搜索和项目 ID。

## 配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `token` | 是 | | 不带 `token` 前缀的个人访问令牌。 |
| `resources` | 是 | | 从上表选择至少一个资源名，每个名只出现一次。 |
| `start_date` | 是 | | 有时间字段的记录输出下界，包含该时刻，格式为 RFC 3339。 |
| `end_date` | 否 | 扫描开始时刻 | 输出上界，不包含该时刻，格式为 RFC 3339。检查点保留该值。 |
| `page_size` | 否 | 50 | 支持 `per_page` 的接口页大小，范围 1–100。发布记录每页最多 10 条。 |
| `max_pages_per_scope` | 否 | 1000 | 每个资源和父范围允许的页数，范围 1–10000。 |
| `max_requests` | 否 | 100000 | 扫描中允许完整处理的成功响应数，范围 1–1000000。 |
| `max_attempts` | 否 | 4 | 每次请求的总尝试次数，范围 1–10。 |
| `request_timeout_ms` | 否 | 30000 | 单次请求的总期限，含重试和解码，范围 100–300000 毫秒。 |
| `max_response_bytes` | 否 | 8388608 | 解码后的响应大小上限，范围 1024–67108864 字节。 |
| `endpoint` | 否 | `https://api.bugsnag.com` | HTTPS 源地址，可设为 `https://api.bugsnag.smartbear.com` 或自建 API 源地址。 |
| `trust_certificate` | 否 | | Worker 本地 X.509 PEM 文件，自定义 HTTPS 源地址时必填。 |

## 输出

`bugsnag.records` 包含七个 `STRING` 列。

| 列 | 说明 |
| --- | --- |
| `resource` | 资源名。 |
| `record_id` | 供应商 `id`、集成 `key`、透视维度 `event_field_display_id`、字段 `display_id`，或用量摘要所属搜索的 ID。 |
| `organization_id` | 适用时为真实组织 ID。 |
| `project_id` | 适用时为真实项目 ID。协作者和团队使用 `organization_id` 表示所属组织。 |
| `search_id` | 已保存搜索及用量摘要的搜索 ID。 |
| `record_time` | 资源所用的供应商时间，统一为 UTC。无时间筛选的资源为 null。 |
| `payload` | 完整供应商对象的 JSON 文本，保留嵌套值、额外字段、null 和数值精度。 |

记录标识的范围是资源及其父层级。BugSnag 返回的项目或组织 API 密钥、用户数据和完整事件详情可能出现在 `payload` 中。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  BugSnag {
    token = ${BUGSNAG_AUTH_TOKEN}
    resources = ["projects", "errors", "events", "releases"]
    start_date = "2025-01-01T00:00:00Z"
    end_date = "2025-02-01T00:00:00Z"
  }
}

sink {
  Console {}
}
```

## 分页、时间范围和恢复

连接器按照响应头中 `Link` 的 `next` 关系翻页，包括不足一页或空页。后续请求须使用相同的 HTTPS 源地址、API 路由和固定查询参数。发布记录未提供下一页链接时，按照 API 定义的数值偏移量，以原始响应记录数前进。发布请求的 `base` 采用冻结的扫描结束时刻。未带页大小参数的列表一次响应最多处理 10000 条。 [分页](https://developer.smartbear.com/bugsnag/docs/data-access-pagination)

四种有时间字段的资源按 `[start_date, end_date)` 输出。读取所有可用页面并保留父级遍历，时间范围只影响输出。可见数据受供应商留存期限和权限约束。这些 Data Access 接口不提供按时点冻结的快照或持续 CDC。

检查点保留扫描结束时刻、待遍历的父层级、已校验的偏移量与已发记录数。恢复时会校验当前页记录和续页位置，再发送剩余记录。令牌、资源、时间范围、源地址、证书路径或页大小变化后需要重新扫描。HTTPS 客户端校验证书和主机名、禁止重定向，并在请求期限内重试。HTTP 429 需要可用的 `Retry-After` 值。 [限流](https://developer.smartbear.com/bugsnag/docs/data-access-rate-limiting)
