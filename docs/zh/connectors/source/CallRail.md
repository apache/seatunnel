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

# CallRail Source

`CallRail` Source 通过 CallRail v3 API 有界读取一个账户，源端并行度为 1。发行包中缺少此连接器时，请安装 `connector-http-callrail`。

| 资源 | API 操作 | 输出时间字段 |
| --- | --- | --- |
| `calls` | `GET /v3/a/{account_id}/calls.json` | `start_time` |
| `conversations` | `GET /v3/a/{account_id}/text-messages.json` | `last_message_at` |
| `users` | `GET /v3/a/{account_id}/users.json` | `created_at` |
| `companies` | `GET /v3/a/{account_id}/companies.json` | `created_at` |

连接器发送 `Authorization: Token token="<token>"`。令牌所属用户在 CallRail 的权限决定可读取的账户数据。通话及短信会话会请求官方支持的归因、格式化信息和消息字段。旧 Cookie 字段 `utma`、`utmb`、`utmc`、`utmv` 和 `utmz` 在供应商返回时会保留，当前字段选择文档未列出这些字段。录音、脚本、消息附件和其他 URL 作为数据保留。 [CallRail API](https://apidocs.callrail.com/) · [认证](https://apidocs.callrail.com/#authorization)

## 配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `token` | 是 | | 个人 CallRail v3 API 密钥。 |
| `account_id` | 是 | | 账户 ID 字符串，接受数字 ID 和数字字母组成的掩码 ID。 |
| `resources` | 是 | | 按顺序选择一个或多个资源名，且名称不重复。 |
| `start_date` | 是 | | 含下界的 UTC 日期或带偏移量的 RFC 3339 时间戳。日期代表 UTC 午夜。 |
| `end_date` | 否 | 扫描开始时刻 | 不含上界的 UTC 日期或带偏移量的 RFC 3339 时间戳。 |
| `calls_pagination` | 否 | 数字账户用 `relative`，掩码账户用 `offset` | 通话的相对分页或数字页码模式。其余资源始终使用数字页码。 |
| `page_size` | 否 | 100 | 每页请求的记录数，范围 1–250。 |
| `max_pages_per_scope` | 否 | 1000 | 每个资源及日期切片允许的最多页数，范围 1–10000。 |
| `max_requests` | 否 | 100000 | 扫描中允许完整处理的成功响应数，范围 1–1000000。 |
| `max_attempts` | 否 | 4 | 每次请求的总尝试次数，范围 1–10。 |
| `request_timeout_ms` | 否 | 30000 | 单页获取总期限，范围 100–300000 毫秒。 |
| `max_response_bytes` | 否 | 8388608 | 解码后的响应大小上限，范围 1024–67108864 字节。 |
| `endpoint` | 否 | `https://api.callrail.com` | HTTPS 源地址。显式配置时需提供 `trust_certificate`。 |
| `trust_certificate` | 否 | | Worker 本地的自定义 HTTPS 源 X.509 PEM 文件路径。 |

## 输出

`callrail.records` 包含五个 `STRING` 列。

| 列 | 说明 |
| --- | --- |
| `resource` | 资源名。 |
| `record_id` | 精确的供应商记录 ID，包括掩码字符串及旧数字 ID。 |
| `account_id` | 源端配置的账户 ID。 |
| `record_time` | 转为 UTC 的供应商时间戳。用户或公司创建时间缺失时为 null。 |
| `payload` | 供应商完整记录的 JSON 文本，保留嵌套值、额外字段和数值精度。 |

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  CallRail {
    token = ${CALLRAIL_API_TOKEN}
    account_id = "227799611"
    resources = ["calls", "conversations", "users", "companies"]
    start_date = "2025-01-01"
    end_date = "2025-04-15"
  }
}

sink {
  Console {}
}
```

## 时间范围、分页和恢复

通话和短信会话以最多 100 天为一段，按 `Etc/UTC` 请求含首尾日期的数据。连接器在本地以完整精度按 `[start_date, end_date)` 筛选。用户和公司读取全部可用页，创建时间仅用于本地筛选，并保留缺失创建时间的记录。可见数据受 CallRail 留存和访问规则限制。 [日期筛选](https://apidocs.callrail.com/#filtering-data-by-date)

数字页码模式使用响应的 `page`、`per_page`、`total_pages` 和 `total_records`。相对分页在 `has_next_page` 为 true 时使用供应商的 `next_page`，并验证原 HTTPS 源、账户路径、日期切片、字段、页大小和相对分页模式。供应商的无后缀通话 URL 也可用于续页。掩码账户若收到包含不同内部账户 ID 的续页地址，可选通话的数字页码模式。不足一页或空页仍可能有后续页。 [数字页码与相对分页](https://apidocs.callrail.com/#relative-pagination) · [数字页码](https://apidocs.callrail.com/#offset-pagination)

检查点保存冻结的时间上界、资源位置、日期切片、分页状态、页指纹和已发送的行数。页内恢复会校验原始页面和后续位置，再发送剩余的行。变更令牌、账户、资源顺序、时间范围、分页配置、源地址或证书路径后需要重新扫描。供应商列表不提供隔离快照或删除日志。

HTTPS 客户端校验证书和主机名，禁止重定向，并限制重试和解码响应大小。若有合法 `Retry-After`，连接器遵守该等待时间。HTTP 429 未给出等待时间时使用指数退避。CallRail 可对同一账户的所有读取请求实施限流。 [限流](https://apidocs.callrail.com/#rate-limiting)
