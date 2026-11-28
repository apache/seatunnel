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

import ChangeLog from '../changelog/connector-http-calendly.md';

# Calendly

安装 `connector-http-calendly` 后，可从 Calendly API v2 批量读取数据。每个 Source 配置块选择一个资源，源端并行度设为 `1`。Source 会核验鉴权用户及其当前组织。凭据需要 `users:read`，以及所选资源和相关父集合的读取权限。组织级读取可能需要所有者或管理员权限，个别资源还取决于套餐或相应功能是否启用。 [Calendly API 参考](https://developer.calendly.com/api-docs/overview/api/reference)

## 资源

| `resource` | 记录 |
| --- | --- |
| `api_user` | 当前鉴权用户 |
| `event_types` | 当前组织的活动类型 |
| `groups` | 当前组织的群组 |
| `organization_memberships` | 组织成员 |
| `scheduled_events` | 已排期及已取消的活动 |
| `event_invitees` | 发现的每个活动下的受邀者 |
| `teams` | 调用者可见的团队 |
| `event_type_memberships` | 各活动类型的成员 |
| `event_type_availability_schedules` | 各活动类型的命名日程或自定义时段 |
| `group_relationships` | 各群组的成员关系 |
| `user_availability_schedules` | 各组织成员的可用日程 |
| `locations` | 各组织成员的会议地点 |
| `organization_invitations` | 当前组织的邀请 |
| `webhook_subscriptions` | 组织级 Webhook 订阅 |
| `routing_forms` | 当前组织的路由表单 |
| `routing_form_submissions` | 各路由表单的提交记录 |
| `contacts` | 调用者可见的联系人 |
| `outgoing_communications` | 组织发送的短信和邮件 |
| `activity_log_entries` | 组织活动日志，需要 Enterprise 权限 |
| `meeting_recaps` | 调用者可见的可用 Notetaker 会议纪要 |

## 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `resource` | string | 必填 | 选择上表中的一个资源。 |
| `token` | string | PAT 鉴权 | 个人访问令牌。使用 PAT 或下列三个 OAuth 配置。 |
| `client_id` | string | OAuth 鉴权 | Calendly OAuth 应用的客户端 ID。 |
| `client_secret` | string | OAuth 鉴权 | Calendly OAuth 应用的客户端密钥。 |
| `oauth_token_file` | string | OAuth 鉴权 | 合格 Worker 上私有且可写的刷新令牌状态文件绝对路径。 |
| `start_date` | string | `1970-01-01T00:00:00Z` | RFC 3339 时间，作为下文所述时间窗口的下界。 |
| `end_date` | string | 首次读取时间 | RFC 3339 游标上界，作业从检查点恢复时保持一致。 |
| `incremental_start` | string | 未设置 | 六个时间游标资源上次导出的 RFC 3339 高水位。 |
| `lookback_days` | int | `0` | `scheduled_events` 从高水位回放时增加的天数，范围 0–3650。 |
| `page_size` | int | `100` | 支持 count 的接口每页行数，范围 1–100。 |
| `max_pages` | int | `2000` | 整个遍历允许接收的数据页总数，含父资源页与空页，范围 1–20000。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接超时，单位毫秒，范围 1–120000。 |
| `read_timeout_ms` | int | `30000` | 套接字读取超时，单位毫秒，范围 1–120000。 |
| `request_deadline_ms` | int | `90000` | 每次 API 读取、OAuth 交换和重试的总时限，单位毫秒，范围 1–300000。 |
| `max_attempts` | int | `4` | GET 最大尝试次数，包含首次请求和 OAuth 恢复后的重放，范围 1–10。 |
| `max_response_bytes` | int | `8388608` | 每个响应编码前后的最大字节数，范围 1024–33554432。 |
| `tls_trust_certificate` | string | JVM 信任库 | 本地 PEM 或 DER X.509 信任锚文件的可选绝对路径。连接始终校验主机名。 |

## 鉴权

请选择一种鉴权方式。使用 PAT 时设置 `token`。使用 OAuth 时，创建 Calendly OAuth 应用并在 SeaTunnel 外完成授权。启动作业前，将获得的刷新令牌写入 UTF-8 JSON 文件，并设置权限为 `0600`。

```json
{"version":1,"refresh_token":"REPLACE_WITH_GRANTED_REFRESH_TOKEN"}
```

配置 `client_id`、`client_secret` 和 `oauth_token_file`。在所有可能执行或恢复该 Source 的 Zeta Worker 上，将同一份持久化令牌文件及其目录挂载到配置的绝对路径。使用支持文件锁、原子重命名以及文件和目录同步的 POSIX 文件系统。文件和目录的所有者应为作业用户，该用户需要目录写权限，用户组和其他用户不得写入该目录。令牌文件和锁文件都应为硬链接计数为 1 的常规文件。Source 会创建同目录的私有 `.lock` 文件。每个令牌文件用于一项部署，重新写入授权令牌前应先停止部署。

每次成功刷新后，Source 会先持久化新的刷新令牌，再使用访问令牌。Basic 凭据只发送到 `auth.calendly.com`，Bearer 凭据只发送到 `api.calendly.com`。令牌 POST 被拒绝或结果无法确认时，状态文件会保留待确认标记，需要重新授权并替换该文件。数据 GET 返回 `401` 时，OAuth 模式可在请求预算内刷新一次并重试同一 GET。 [Calendly 刷新令牌轮换](https://developer.calendly.com/docs/authentication/refresh-token-rotation-guide)

## 时间窗口

| 资源 | 游标字段 | 初次读取下界 | API 范围和回放 |
| --- | --- | --- | --- |
| `event_types` | `updated_at` | 1970-01-01 | 无 |
| `scheduled_events` | `updated_at` | 1970-01-01 | 下发 `min_start_time=start_date`，从高水位回放时回看五分钟和 `lookback_days` 天 |
| `event_invitees` | `created_at` | `start_date` | 发现父活动时包含早于 `start_date` 开始的活动 |
| `outgoing_communications` | `created_at` | `start_date` | `min_created_at` 和 `max_created_at` |
| `activity_log_entries` | `occurred_at` | `start_date` | `min_occurred_at` 和 `max_occurred_at` |
| `meeting_recaps` | `start_time` | `start_date` | `start_time` 和 `end_time`，从高水位回放时回看三天 |

新的作业可以通过 `incremental_start` 传入下游保存的高水位。回放下界最早退至初次读取下界。Source 会按本地游标检查每条返回记录，包含窗口的上下界。服务端时间过滤也决定哪些记录可以进入这一步。例如，`scheduled_events` 会排除早于 `start_date` 开始的会议，即使会议随后有更新。读取受邀者时，父活动发现不会使用这个开始时间过滤。即使某页的时间都早于下界，Source 仍会继续读取分页。跨作业回看可能再次输出重叠窗口中的记录。

## 输出和恢复

| STRING 列 | 说明 |
| --- | --- |
| `id` | 存在规范的 Calendly API 记录 URI 时，使用其路径的末段。可用日程使用命名规则的 URI。自定义时段、地点和通知记录的 ID 可以为空。 |
| `parent_id` | 请求范围的 ID，例如组织、活动类型、用户、群组、活动或路由表单。没有请求范围时为空。 |
| `updated_at` | 六个时间窗口资源的游标转换为 UTC。其余资源有 `updated_at` 时保留原值。 |
| `data` | 完整 JSON 对象，保留原始数值、嵌套值和空值。为活动和受邀者增加 `id`，为受邀者增加 `scheduled_event_id`，为地点增加 `user`，为活动类型可用日程增加 `availability_rule_uri`。 |

分页只使用不透明的 `next_page_token`，空页也可携带下一页游标。响应中的 URL 仅作为数据保留。连接使用 TLS 并校验 API 主机名，遇到重定向会失败。Source 按配置的限额重试安全 GET，并在限流时遵守 `Retry-After` 或 `X-RateLimit-Reset`。发现的子资源如果在首个子页读取前消失，可跳过该项。普通 `403` 等权限或套餐错误会使作业失败。 [Calendly API 限流](https://developer.calendly.com/api-docs/overview/rate-limits)

从检查点恢复时，Source 会核验已保存的用户及组织身份，然后重读当前的父页和子页。重读页的记录和下一页游标都匹配保存状态后，才会从保存的偏移量继续。已读完的分片恢复时无需鉴权或 API 请求。服务端数据可在分页期间变化，同一次作业可能读取到期间的更新。检查点用于恢复同一次作业的进度，独立的后续作业需显式传入下游保存的高水位。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Calendly {
    resource = "scheduled_events"
    token = ${CALENDLY_PERSONAL_ACCESS_TOKEN}
    start_date = "2025-01-01T00:00:00Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
