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

import ChangeLog from '../changelog/connector-http-bigmailer.md';

# BigMailer

安装 `connector-http-bigmailer` 后，在 Source 并行度为 1 的批任务中配置 `BigMailer`。每个 Source 实例从 [BigMailer v1 API](https://docs.bigmailer.io/docs/getting-started-api) 读取一种资源。在 BigMailer 创建可访问目标账号和资源的 API 密钥。

| 资源 | 记录 |
| --- | --- |
| `brands` | 账号可见的品牌。 |
| `users` | 账号可见的用户记录。 |
| `contacts` | 联系人、联系人字段和所属列表。 |
| `lists` | 联系人列表。 |
| `fields` | 联系人字段定义。 |
| `message-types` | 消息类型和邮件偏好定义。 |
| `segments` | 已保存的联系人分群。 |
| `bulk_campaigns` | 已有的批量营销活动。 |
| `transactional_campaigns` | 已有的事务型营销活动。 |
| `suppression_lists` | 营销活动抑制列表的文件元数据。 |

前两种资源在账号范围内读取。其余资源会先发现所有可见品牌，逐品牌读取对应资源并保留品牌归属。无需另外配置品牌 Source。所有请求均为 GET，只访问上表中的资源。

输出有两个 `STRING` 列。`brand_id` 对品牌子资源表示所属品牌，对 `brands` 表示品牌自身 ID，对 `users` 为 null。`data` 是 JSON 对象，保留供应商的字段名、嵌套值、数字精度和显式 null。响应中缺省的字段仍保持缺省。使用品牌子资源的 ID 时，应同时考虑 `brand_id`。

## 选项

| 选项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `token` | string | — | 必填，BigMailer API 密钥，通过 `X-API-Key` 发送。 |
| `resource` | string | — | 必填，以上资源名之一。 |
| `page_size` | int | `100` | 每页请求的记录数，范围为 1–100。 |
| `max_pages` | int | `2000` | 每次账号级列表或每个品牌子列表的最大页数，范围为 1–10000。品牌发现单独计数。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接超时，范围为 1–120000 毫秒。 |
| `read_timeout_ms` | int | `30000` | Socket 读取超时，范围为 1–120000 毫秒。 |
| `request_deadline_ms` | int | `60000` | 含读取响应体及重试在内的请求总时限，范围为 1–300000 毫秒。 |
| `max_attempts` | int | `4` | 含首次请求在内的最大尝试次数，范围为 1–10。 |
| `max_response_bytes` | int | `8388608` | 响应在传输和解压后的独立大小上限，范围为 1024–33554432。 |

分页以供应商返回的 `has_more` 为准，短页和空页也可能继续。缺少游标、游标重复、权限拒绝或超过页数上限时，读取会失败。Source 校验 `api.bigmailer.io` 的 TLS 证书和主机名，并拒绝重定向。每个 Source 实例同一时间只发送一个请求，并限制请求间隔。多个任务和应用可能共同占用账号配额。重试只在总时限内按 `Retry-After` 等待。

新任务执行一次有界全量读取。Zeta 恢复时使用已保存的品牌发现和数据位置，状态与 API 密钥、资源和页大小绑定。应用保存的行偏移前，Source 会核对正在读取的页。如果该页数据、有序品牌 ID 或续页标记发生变化，恢复会明确失败。已经读完的页不会重新获取。读取过程中供应商数据可能变化，恢复无法提供整个账号的一致性快照。已完成的资源恢复后仍保持完成状态。更换 API 密钥后应启动新读取。

## 示例

```hocon
env {
    job.mode = "BATCH"
    parallelism = 1
}
source {
    BigMailer {
        token = ${BIGMAILER_TOKEN}
        resource = "contacts"
        page_size = 100
    }
}
sink {
    Console {}
}
```

<ChangeLog />
