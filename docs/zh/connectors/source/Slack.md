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

import ChangeLog from '../changelog/connector-slack.md';

# Slack

通过 Slack `conversations.history` Web API 有界读取会话历史消息。

## 支持范围

批处理 Source，源并行度必须为 **1**。在现有 `connector-slack` 插件中提供固定表结构、游标分页及检查点恢复。Source 使用默认脱敏的 `token` 凭证参数，既有 Slack Sink 仍使用 `oauth_token`，配置保持有效。

仅读取明确指定的会话，不支持 Events API 流式订阅、会话发现、`conversations.replies` 线程回复遍历、文件、用户列表或其他 Slack 数据流。输出此 API 实际返回的历史条目；可访问数据受工作区保留策略、令牌权限和 Slack API 限制约束。

Bot 必须加入各会话并具备相应的 `channels:history`、`groups:history`、`im:history` 或 `mpim:history` 权限。用户令牌遵循 Slack 的访问规则。私有会话仍需满足成员身份要求，连接器不会扩大令牌权限。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| token | string | 是 | 无 | 只通过 Authorization 请求头发送；使用密钥提供程序。 |
| channel_ids | list(string) | 是 | 无 | 1–10000 个不重复的字母数字会话 ID。 |
| api_root | string | 否 | https://slack.com/api/ | 可信 API 根地址，以 `/` 结尾，无用户信息、查询参数或片段。HTTP 仅供测试夹具使用；拒绝所有重定向。 |
| page_size | int | 否 | 15 | 1–999；Slack 可以返回更少条目或限制实际页大小。 |
| oldest | string | 否 | 无 | 不包含边界的最早 Unix 时间戳字符串。 |
| latest | string | 否 | 创建 Source 的时间 | 不包含边界的最晚 Unix 时间戳，冻结后写入检查点。 |
| request_timeout_ms | int | 否 | 30000 | 单次完整 HTTP 请求截止时间，1–300000 毫秒。 |
| max_retries | int | 否 | 3 | 传输错误、HTTP 429 和 5xx 的额外尝试次数，0–10。 |
| retry_backoff_ms | int | 否 | 1000 | 固定重试间隔，1–max_retry_delay_ms 毫秒。 |
| max_retry_delay_ms | int | 否 | 120000 | 1–300000 毫秒；Retry-After 无效或超限时失败，避免提前重试。 |
| max_response_bytes | int | 否 | 4194304 | 每页解压后字节上限，1–67108864，超限失败。 |
| max_pages_per_channel | int | 否 | 10000 | 每个会话 1–100000 页，含空页；超限失败而非截断数据。 |

时间戳使用最多 40 字符的非负十进制字符串，如 `"1700000000.000001"`，不转换成浮点数。`oldest` 必须小于 `latest`。两个边界均不包含（`inclusive=false`）。不要配置自定义 schema，输出固定为下表。

## 输出结构

全部字段为 `STRING`，顺序如下。缺失的可选字段输出 null，保留 Unicode。

| 字段 | 含义 |
| --- | --- |
| channel_id | 配置的会话 ID，非空。 |
| ts | Slack 原始时间戳字符串，非空，保留全部小数位。 |
| thread_ts | 线程时间戳，缺失时 null。 |
| user | 用户 ID，缺失时 null。 |
| text | 消息文本，缺失时 null。 |
| type | 消息或事件类型，缺失时 null。 |
| subtype | 消息子类型，缺失时 null。 |
| raw_json | 完整消息 JSON，非空；不保留原始空白格式。 |

## 分页、失败和恢复

只要 `response_metadata.next_cursor` 非空，短页和空页都会继续请求。空游标结束当前会话。`has_more=true` 却没有可用游标、响应格式错误或游标重复时，在输出该页前失败。游标最长 4096 字符。HTTP 200 的 `ok:false`、永久 HTTP 错误和重定向均为失败，不会被解释成空历史；日志不输出响应正文、令牌或 Authorization。

状态版本 2 保存会话位置、当前游标、冻结的 latest，以及当前页已接受记录数和八个实际输出字段的 SHA-256 摘要。完整响应及前缀校验不持有检查点锁。每次轮询最多接受一条已准备记录后即交还引擎，最后一行与游标或会话切换、前缀重置一起原子提交。网络读取和重试等待由一个专属线程执行，最多一个待处理请求。单次输出器调用阻塞时仍遵循引擎的检查点及取消行为。恢复时重新读取当前页，验证全部已接受前缀后才跳过；前缀内容、顺序或长度变化时在新输出前失败。不支持早期开发阶段的检查点格式。最近完成检查点之后接受的记录仍可重放，结果取决于下游恢复语义。下游可用 `(channel_id, ts)` 做幂等键，本 Source 不承诺 Sink 端精确一次交付。冻结 latest 不等于事务快照，未接受记录、保留策略、删除及游标过期仍可改变或阻止恢复。恢复同一任务时不要修改会话顺序或时间窗口。

CI 的 API 契约 E2E 使用确定性 HTTP 夹具执行真实引擎作业，不代表真实 Slack 工作区或令牌认证通过。部署时另行确认权限、成员身份、数据保留和 API 限流。参考官方 [history API](https://docs.slack.dev/reference/methods/conversations.history/) 与 [限流规则](https://docs.slack.dev/apis/web-api/rate-limits/)。

## 示例

```hocon
env { parallelism = 1, job.mode = "BATCH" }
source {
  Slack {
    token = ${SLACK_OAUTH_TOKEN}
    channel_ids = ["C0123456789", "C9876543210"]
    oldest = "1700000000.000001"
    latest = "1700086400.000001"
    page_size = 15
  }
}
sink { Console {} }
```

## 变更日志

<ChangeLog />
