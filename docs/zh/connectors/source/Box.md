import ChangeLog from '../changelog/connector-http-box.md';

{/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */}

# Box

安装 `connector-http-box` 后，使用 `Box` 作为 Source 标识。它读取 Box 账户资源，也可以对已有文件提取文本或调用 Box AI 获取结果。数据源使用一个有界分片，源端并行度为 1。[Box API 文档](https://developer.box.com/reference/)

## 资源

| 资源 | 读取范围 |
| --- | --- |
| `events`、`admin_logs` | 用户事件与企业历史事件。企业历史需要管理员权限。 |
| `sign_templates`、`sign_request` | 当前账户可见的 Box Sign 模板和请求。 |
| `collections`、`collection_items` | 当前账户可见的集合及其中的项目。 |
| `files`、`folders` | Box 根目录的直接文件和文件夹。 |
| `file_collaborations`、`file_comments`、`file_tasks` | 根目录直接文件的协作者、评论和任务。 |
| `folder_collaborations` | 根目录直接子文件夹的协作者。 |
| `recent_items`、`trashed_items`、`users` | 最近访问的项目、回收站项目与可见的企业用户。 |
| `text_representation` | 由 Box 为 `folder_id` 中的文件提取的 UTF-8 文本。 |
| `ai_ask`、`ai_extract`、`ai_extract_structured` | 对 `folder_id` 中的文件逐个执行 AI 问答、自由提取或结构化提取。 |

选择子资源时，连接器会发现对应的上级资源，即使上级资源不在输出范围内。提取文件时可递归遍历。文件链接、协作和分享 URL 作为数据保留在 JSON 中，可能包含敏感信息。

输出表 `records` 包含五列，类型均为 STRING。

| 字段 | 说明 |
| --- | --- |
| `resource` | 所选 Box 资源名。 |
| `record_id` | 供应商记录 ID。事件使用 `event_id`，最近访问项目优先使用底层项目类型和 ID。 |
| `parent_id` | 适用时填写所属文件夹、集合或文件的 ID。 |
| `payload` | 目录记录或文件详情的完整 JSON。 |
| `content` | 提取的文本、Box AI 答案或结构化 AI 响应的完整 JSON。目录记录此列为 null。 |

## 配置项

| 配置项 | 类型 | 默认值 | 要求和行为 |
| --- | --- | --- | --- |
| `resources` | LIST of STRING | | 必填，从上表选择至少一个资源，每项仅可选一次。 |
| `auth_mode` | STRING | `bearer` | 使用传入的 API 令牌，或配置 `client_credentials`。 |
| `token` | STRING | | `bearer` 模式必填。 |
| `client_id` | STRING | | `client_credentials` 模式必填。 |
| `secret_key` | STRING | | `client_credentials` 模式必填，以 `client_secret` 表单字段发送给授权端点。 |
| `box_subject_type` | STRING | | `client_credentials` 模式必填，使用 `user` 或 `enterprise`。 |
| `box_subject_id` | STRING | | `client_credentials` 模式必填，填写十进制用户或企业 ID。 |
| `endpoint` | STRING | `https://api.box.com` | 自定义 HTTPS API 源地址，不包含路径、查询参数、片段或凭据，必须配置 `trust_certificate`。 |
| `download_endpoint` | STRING | | 自定义 HTTPS 内容源地址，需要同时配置自定义 API 源地址和 `trust_certificate`。 |
| `trust_certificate` | STRING | | 执行节点上的自定义源 PEM 信任证书。连接器继续验证主机名。 |
| `page_size` | INT | 100 | 每页请求条数，范围 1 至 500。 |
| `max_pages_per_parent` | INT | 1000 | 每个上级资源下的每类资源分页上限，范围 1 至 10000。 |
| `max_requests` | INT | 50000 | API 和内容逻辑请求上限，范围 1 至 1000000。认证与重试分别受限。 |
| `max_attempts` | INT | 4 | 单个可重试请求的总尝试次数，范围 1 至 10。 |
| `request_timeout_ms` | INT | 30000 | 单次获取总超时，包含重试和轮询，范围 100 至 300000 毫秒。 |
| `max_response_bytes` | INT | 8388608 | 成功响应解码后的最大字节数，范围 1024 至 67108864。 |
| `events_start_position` | STRING | `0` | 用户事件初始位置，填写十进制数或 `now`。 |
| `admin_start_position` | STRING | `0` | 企业历史初始位置，填写十进制数或 `now`。 |
| `admin_created_after` | STRING | | 企业历史事件可选起始时刻，使用 RFC 3339。 |
| `admin_created_before` | STRING | | 企业历史事件可选结束时刻，使用 RFC 3339。 |
| `folder_id` | STRING | `0` | 文件提取的根目录 ID。 |
| `recursive` | BOOLEAN | false | 对所选文件提取资源遍历子文件夹。 |
| `max_depth` | INT | 32 | 从配置文件夹起允许的提取深度，范围 0 至 64。 |
| `max_items` | INT | 10000 | 文件提取时允许发现的原始条目数，范围 1 至 100000。 |
| `max_representation_polls` | INT | 8 | 单个文件的文本生成状态请求上限，范围 1 至 100。 |
| `ask_prompt` | STRING | | 选择 `ai_ask` 时必填，最多 10000 字符。 |
| `extract_prompt` | STRING | | 选择 `ai_extract` 时必填，最多 10000 字符。 |
| `structured_fields` | STRING | | 选择 `ai_extract_structured` 时必填。内容为定义提取字段的 JSON 数组。 |
| `file_fields` | LIST of STRING | | 可选文件详情字段清单。配置后，连接器始终包含 `id`、`type` 和 `name`。 |

客户端凭据模式需要 Box 应用有权访问所选主体和资源，连接器在内存中获取并更新访问令牌。直接传入的 Bearer 令牌可能在扫描期间过期。文件、企业管理记录和 AI 功能受 Box 账户权限和功能订阅控制。[Box 应用及令牌权限](https://developer.box.com/guides/security)

## 分页和文件提取

支持 marker 分页的资源使用 marker。集合、集合条目和文件评论使用 offset，按 Box 返回的 limit 推进。短页或空页带有后续位置时会继续分页。仅支持 offset 的接口如需超出 Box 的 9999 上限，作业会明确失败。事件使用 `stream_position`，空事件批次表示结束。用户事件流可能多次返回同一个事件 ID。[Box 分页](https://developer.box.com/guides/api-calls/pagination/) [Box 企业事件](https://developer.box.com/guides/events/enterprise-events/for-enterprise)

文本提取使用 `extracted_text`。Box 可能异步生成文件文本。状态请求只发往 API 源地址。内容请求可访问 Box 官方内容域名 `dl.boxcloud.com` 和 `public.boxcloud.com`，或显式配置的自定义内容源地址。连接器拒绝重定向并验证主机名。配置自定义 API 源地址后，要启用内容请求还需配置 `download_endpoint`。Box 未列出文本时，文件的提取文本为空。状态、权限或正文请求错误会让作业失败。[请求文件表示](https://developer.box.com/guides/representations/request-a-representation) [下载文件表示](https://developer.box.com/guides/representations/download-a-representation)

文件详情可通过 `file_fields` 选择 `permissions` 和 `metadata.enterprise_12345.invoice` 等字段。结构化提取可以指定 `key`、可选的 `type`、`description`、`displayName` 或 `display_name`、`prompt` 和 `options`。各字段独立配置选项，选项使用不重复的 `key`。可使用嵌套 `fields` 定义结构或表格。自由提取也支持在 `extract_prompt` 中提供模式。连接器只向 Box 传入已有文件的标识以及配置的提示或字段定义，费用与文件格式支持取决于 Box。[结构化 AI 提取](https://developer.box.com/reference/post-ai-extract-structured)

## 恢复

检查点保留尚未处理的上级请求和供应商的精确续页位置。已输出一部分的页面在恢复后会先校验，再输出剩余记录。查询范围变化、重复续页位置和递归循环会明确失败。扫描期间对 Box 账户的修改仍可能影响列表结果。

HTTP 429 附带有效 `Retry-After` 时，可以在配置限制内重试。AI POST 的结果不确定时，连接器仅报告失败。在作业恢复后，未完成检查点的提取可能再次产生费用。检查点不保存凭据、提示或文件正文。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Box {
    resources = ["files", "file_collaborations", "text_representation", "ai_extract_structured"]
    auth_mode = "client_credentials"
    client_id = ${BOX_CLIENT_ID}
    secret_key = ${BOX_CLIENT_SECRET}
    box_subject_type = "enterprise"
    box_subject_id = "12345"
    folder_id = "9001"
    recursive = true
    file_fields = ["permissions", "metadata.enterprise_12345.invoice"]
    structured_fields = """[{"key":"invoice_number","type":"string"}]"""
  }
}
```

## 变更日志

<ChangeLog />
