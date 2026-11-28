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

import ChangeLog from '../changelog/connector-http-boldsign.md';

# BoldSign

安装 `connector-http-boldsign` 后，把 `BoldSign` 配置为并行度 1 的批量 Source。每个 Source 实例通过 [BoldSign API](https://developers.boldsign.com/api-overview/getting-started/) 读取一种资源。使用与[账号所在数据区域](https://support.boldsign.com/kb/article/14423/resolving-401-unauthorized-error)匹配的 API 密钥。

| 资源 | 记录 |
| --- | --- |
| `documents` | My Documents 中可见的文档元数据。 |
| `brands` | API 密钥可访问的所有品牌。 |
| `senderIdentities` | 可访问的发件人身份。 |
| `teams` | 团队记录。 |
| `templates` | 模板元数据。 |
| `users_list` | 账号用户记录。 |
| `custom_fields` | 各已发现品牌的自定义字段。 |
| `contacts` | 供应商默认联系人范围内可访问的联系人。 |

连接器通过 GET 读取列表。读取 `custom_fields` 时自动发现品牌。每行包含两个 `STRING` 字段。`brand_id` 仅在自定义字段资源中填充，其余资源为 null。`data` 是 BoldSign 返回的 JSON 对象，保留字段名、嵌套值、数字精度、显式 null 和缺省字段。

## 选项

| 选项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `token` | string | — | 必填，BoldSign API 密钥，通过 `X-API-KEY` 发送。 |
| `resource` | string | — | 必填，以上资源名之一。 |
| `region` | string | `us` | 账号所在区域，可选 `us`、`eu`、`ca` 和 `au`。 |
| `start_date` | string | — | 可选，文档、团队、模板和用户的创建时间下限，包含该时刻。 |
| `end_date` | string | 指定 `start_date` 时默认为任务初始化时间 | 可选，同一批资源的创建时间上限，包含该时刻。 |
| `page_size` | int | `50` | 每页请求的记录数，范围为 1–100。对文档资源，此值须能整除 10,000。 |
| `max_pages` | int | `2000` | 每个列表的最大页数，含文档续读窗口，范围为 1–20000。 |
| `max_unpaged_records` | int | `10000` | 单个品牌或自定义字段响应的行数上限，范围为 1–50000。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接超时，范围为 1–120000 毫秒。 |
| `read_timeout_ms` | int | `30000` | Socket 读取超时，范围为 1–120000 毫秒。 |
| `request_deadline_ms` | int | `60000` | 含重试在内的请求总时限，范围为 1–300000 毫秒。 |
| `max_attempts` | int | `4` | 含首次请求在内的最大尝试次数，范围为 1–10。 |
| `max_response_bytes` | int | `8388608` | 响应在传输和解压后的独立大小上限，范围为 1024–33554432 字节。 |

日期采用带明确时区偏移的 ISO 8601 整秒时间。配置任一日期时，连接器在本地根据响应中以 Epoch 秒表示的 `createdDate` 过滤。供应商返回的全部记录均参与分页。显式设置过滤范围时，缺少创建时间的记录会使任务报错，因为无法判断它是否在区间内。

Source 通常按页号读取。文档读满 10,000 条后，通过 `NextCursor` 续读，并重取窗口末尾的相同游标组，使相同游标值的记录能够继续读出，同时避免重复输出已经处理的 ID。如果供应商不再返回需要核对的重叠记录，或整个窗口共享一个游标而无法推进，读取会明确失败。品牌和自定义字段各使用一个不分页的响应。

Zeta 检查点保存已固定的创建时间区间和读取位置。恢复时，Source 先核对正在读取的页或品牌顺序，再使用保存的偏移。更换 API 密钥、区域、资源、页大小或日期选项时，应启动新的读取。每次新读取均使用供应商当时可见的数据。此接口不提供一致性快照或删除事件。连接器校验 TLS 并拒绝重定向。

## 示例

```hocon
env {
    job.mode = "BATCH"
    parallelism = 1
}
source {
    BoldSign {
        token = ${BOLDSIGN_API_KEY}
        region = "us"
        resource = "documents"
        start_date = "2025-01-01T00:00:00Z"
    }
}
sink {
    Console {}
}
```

<ChangeLog />
