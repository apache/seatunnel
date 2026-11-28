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

import ChangeLog from '../changelog/connector-http-aircall.md';

# Aircall

安装 `connector-http-aircall`，在批处理作业使用 `Aircall`。填写 Aircall 控制台中的 API ID 和 API Token。连接器使用 Basic 认证接入私有账户，校验 HTTPS 证书和主机名，并拒绝重定向。[Aircall 认证说明](https://developer.aircall.io/docs/authentication)

## 资源

| 资源 | `/v1` 下的 GET 路由 | 选择方式 |
| --- | --- | --- |
| `calls` | `/calls` | 标准 |
| `company` | `/company` | 标准 |
| `contacts` | `/contacts` | 标准 |
| `numbers` | `/numbers` | 标准 |
| `tags` | `/tags` | 标准 |
| `user_availability` | `/users/availabilities` | 标准 |
| `users` | `/users` | 标准 |
| `teams` | `/teams` | 标准 |
| `webhooks` | `/webhooks` | 标准 |
| `conversations` | `/conversations` | 标准 |
| `conversation_engagements` | `/conversations/{id}/engagements` | 标准 |
| `campaigns` | `/campaigns` | 标准 |
| `campaign_outcomes` | `/campaign_outcomes` | 标准 |
| `trackers` | `/trackers` | 标准 |
| `sms_templates` | `/sms/templates` | 标准 |
| `whatsapp_templates` | `/numbers/{id}/templates` | 标准 |
| `user_numbers` | `/users/{id}/numbers` | 标准 |
| `dialer_campaigns` | `/users/{id}/dialer_campaign` | 标准 |
| `dialer_campaign_phone_numbers` | `/users/{id}/dialer_campaign/phone_numbers` | 标准 |
| `call_transcriptions` | `/calls/{id}/transcription` | 启用 AI 选项 |
| `call_sentiments` | `/calls/{id}/sentiments` | 启用 AI 选项 |
| `call_topics` | `/calls/{id}/topics` | 启用 AI 选项 |
| `call_summaries` | `/calls/{id}/summary` | 启用 AI 选项 |
| `call_action_items` | `/calls/{id}/action_items` | 启用 AI 选项 |
| `call_playbook_results` | `/calls/{id}/playbook_result` | 启用 AI 选项 |

未指定资源列表时会读取全部 19 个标准资源。配置 `include_conversation_intelligence = true` 后可同时读取六项 AI 资源。所选端点需具有 Aircall 计划和读取权限，Playbook 结果需要 AI Assist Pro。用 `resources` 选择账户中要读取的端点。选择子资源时，Source 会发现所需的通话、号码、用户或会话。服务端明确指出资产尚不可用、没有活动拨号活动或号码未配置 WhatsApp 时，可结束对应的子请求。认证或计划权限错误会使作业失败。[Aircall API 参考](https://developers.aircall.io/api-references) · [Playbook 结果](https://developer.aircall.io/docs/ai-playbook-results)

每行包含 `resource STRING`、`id STRING`、可为空的 `parent_id STRING`、可为空的 `call_started_at BIGINT`（Unix 秒），以及 `record STRING`。最后一列保存供应商完整的 JSON 记录，保留嵌套、Unicode、空值和数值精度。Webhook 使用 `webhook_id` 作为标识。记录中带有的元数据 URL 原样作为值输出。

## 配置项

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `username` | String | 必填 | API ID |
| `password` | String | 必填 | API Token |
| `base_url` | String | `https://api.aircall.io` | `/v1` 之前的 HTTPS 源站 |
| `resources` | String 列表 | 全部已启用项 | 选择作业读取的资源，名称不可重复 |
| `include_conversation_intelligence` | Boolean | `false` | 启用六项可选 AI 资源 |
| `start_time` | String | 取决于资源 | 精确到秒的 UTC 历史起点 |
| `end_time` | String | Coordinator 启动时间 | 不包含该时刻的扫描上界，恢复时保留原值 |
| `lookback_days` | Integer | `3` | 通话和会话重读的重叠范围，0 至 30 天 |
| `call_window_days` | Integer | `1` | 通话初始查询窗口，1 至 365 天 |
| `contact_window_days` | Integer | `30` | 联系人初始查询窗口，1 至 365 天 |
| `conversation_window_days` | Integer | `30` | 会话初始查询窗口，1 至 365 天 |
| `page_size` | Integer | `50` | 标准及活动分页大小，1 至 50 |
| `sms_page_size` | Integer | `100` | SMS 模板页大小，1 至 100 |
| `max_pages` | Integer | `10000` | 每个集合分段最多读取的页数，1 至 10000 |
| `request_interval_ms` | Integer | `550` | 本作业相邻请求的最小间隔，500 至 60000 毫秒 |
| `max_attempts` | Integer | `4` | 单页尝试次数上限，包含首次请求，1 至 8 |
| `socket_timeout_ms` | Integer | `15000` | 连接和读取空闲时限，100 至 120000 毫秒 |
| `page_deadline_ms` | Integer | `60000` | 包含重试和解析的单页总时限，500 至 600000 毫秒 |
| `max_response_bytes` | Integer | `8388608` | 解压后响应大小上限，128 至 33554432 字节 |

## 时间范围、分页和恢复

通话默认向前扫描 365 天并包含重叠区间。供应商 API 最多返回其保留的六个月。请求时间依据通话创建时间划分，输出通话以整个区间内的 `started_at` 为准。如果创建与开始时间间隔较大，可扩大回看或历史范围。联系人和会话默认从 2014 年 1 月开始，以 30 天为初始分段。会话时间条件使用 `last_engagement_at`，分配和状态变化不会更新它。Source 发现对应会话后读取其互动记录，子请求不带时间筛选。

Source 校验编号分页链接，并使用限定在各端点的活动游标和 WhatsApp Token。可能达到 10000 条上限时会拆分支持时间条件的查询。如果单秒区间或不支持时间条件的端点可能触及上限，作业会报告错误。重试会在总时限内遵守有效的服务端重试和重置头。Aircall 账户的速率额度由多个 API 调用者共享。[Aircall 分页上限](https://support.aircall.io/en-gb/articles/34702023578397) · [Aircall API 参考](https://developers.aircall.io/api-references)

检查点保存扫描上界、当前父子位置，以及部分已输出页的摘要。恢复时需使用相同的源站、资源选择、时间窗口和分页大小。Source 验证重取的页面之后跳过已输出的行。集合内容变化可能影响后续结果，重叠扫描可能再次读取上一作业输出的记录。API 不提供跨页快照。

## 示例

```hocon
env {
  job.mode = "BATCH"
}

source {
  Aircall {
    username = "${aircall_api_id}"
    password = "${aircall_api_token}"
    resources = ["calls", "contacts", "call_transcriptions"]
    include_conversation_intelligence = true
    start_time = "2026-01-01T00:00:00Z"
    end_time = "2026-02-01T00:00:00Z"
  }
}

sink {
  Console {}
}
```

<ChangeLog />
