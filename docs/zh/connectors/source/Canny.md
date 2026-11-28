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

import ChangeLog from "../changelog/connector-http-canny.md";

# Canny

Canny 源通过 https://canny.io 的只读 JSON POST 接口读取工作区 API 密钥可访问的数据。可以选择要导出的资源，也可以保留空列表以读取全部十种资源。[Canny API 参考](https://developers.canny.io/api-reference)。

| 资源 | API 路径 | 分页方式 |
| --- | --- | --- |
| `boards` | `/api/v1/boards/list` | 一次返回完整看板列表 |
| `categories` | `/api/v1/categories/list` | 对每个已发现的看板使用偏移量 |
| `changelog_entries` | `/api/v1/entries/list` | 变更日志偏移量 |
| `comments` | `/api/v2/comments/list` | 游标 |
| `companies` | `/api/v2/companies/list` | 游标 |
| `posts` | `/api/v1/posts/list` | 偏移量 |
| `status_changes` | `/api/v2/status_changes/list` | 游标 |
| `tags` | `/api/v1/tags/list` | 偏移量 |
| `users` | `/api/v2/users/list` | 游标 |
| `votes` | `/api/v2/votes/list` | 游标 |

只选择分类时，连接器会先发现看板，但只输出分类记录。其他所选资源按原生工作区范围各读取一次。短页或空页之后仍根据 API 的续页标志继续。变更日志先请求 `limit=1000` 和 `skip=0`，需要续页时使用 `limit=10000` 和 `skip=1000`。若第二页后仍有数据，连接器会明确报错，因为 API 允许的最大偏移量为 1000。

## 输出字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | STRING | 所选资源名称 |
| `id` | STRING | 记录的原生 ID |
| `board_id` | STRING | 记录所属看板的 ID。分类记录使用扫描时发现的看板 ID |
| `post_id` | STRING | 关联帖子 ID。对帖子记录，该值等于 `id` |
| `record` | STRING | 原生 JSON 对象，保留嵌套数据和显式 JSON null |

下游需要区分不同看板的分类时，可组合使用 `resource`、`id` 和 `board_id`。原始记录可能包含个人信息。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 | | 工作区 API 密钥，以 `apiKey` 字段写入 HTTPS 请求体 |
| `resources` | 字符串列表 | 否 | `[]` | 使用资源表中的名称。空列表选择全部十种资源 |
| `tls_trust_certificate` | String | 否 | | 源连接器专用的可选 PEM 或 base64 DER X.509 信任证书。始终验证 HTTPS 主机名 |
| `page_size` | Integer | 否 | `100` | 每页请求 1 至 100 条，看板和变更日志使用各自的接口规则 |
| `max_pages_per_collection` | Integer | 否 | `10000` | 单个资源或单个看板分类的最大页数，范围 1 至 100000 |
| `max_pages_per_scan` | Integer | 否 | `100000` | 单次扫描接受的最大响应数，范围 1 至 1000000 |
| `max_unpaginated_items` | Integer | 否 | `10000` | 完整列表中允许的最大看板数，范围 1 至 100000 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时，范围 100 至 300000 毫秒 |
| `request_timeout_ms` | Integer | 否 | `30000` | 单次 HTTPS 请求期限，范围 100 至 300000 毫秒 |
| `operation_timeout_ms` | Integer | 否 | `120000` | 单页全部请求、限速等待和重试的总期限。至少等于 `request_timeout_ms`，最多 600000 毫秒 |
| `request_interval_ms` | Integer | 否 | `3700` | 两次请求间的最小间隔，范围 0 至 60000 毫秒。默认值考虑 Free 套餐对单个读取器的每小时限制 |
| `max_retries` | Integer | 否 | `3` | 临时请求失败时的额外尝试次数，范围 0 至 8 |
| `max_retry_delay_ms` | Integer | 否 | `60000` | 允许的最大重试等待，范围 250 至 600000 毫秒。超过上限时读取失败 |
| `max_response_bytes` | Integer | 否 | `8388608` | 传输及解压后响应体的字节上限，范围 256 至 16777216 |

HTTP 429 优先使用 `Retry-After`。该响应头缺失时使用 Canny 限速重置时间。使用同一密钥或出口 IP 的其他作业也可能占用额度。

## 批处理与恢复

使用 `BATCH` 模式。一个读取器按顺序扫描资源，统一控制密钥请求速率和看板遍历。检查点保存当前资源、响应位置和恢复所需的续页信息。恢复时先校验已接受的页面，检查点中不保存 API 密钥或记录内容。修改密钥、资源选择或页大小后，需发起新的扫描。Canny 的数据可以在两次请求间变化，检查点不能提供整个工作区的同一时间点快照。已保存的页面在重读前发生变化时，作业会报告检查点错误。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Canny {
    token = "${CANNY_TOKEN}"
    resources = ["boards", "categories", "posts", "votes"]
  }
}
sink {
  Console {}
}
```

<ChangeLog />
