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

import ChangeLog from '../changelog/connector-http-algolia.md';

# Algolia

安装 `connector-http-algolia`，在批处理任务中配置 `Algolia`。API 密钥需要具备所选资源对应的权限。连接器使用经过验证的 HTTPS 访问 Algolia Search REST API，通过请求头发送应用 ID 和密钥，并拒绝重定向。 [Algolia Search API](https://www.algolia.com/doc/rest-api/search)

## 资源

| 资源 | REST 路径 | 权限 |
| --- | --- | --- |
| `indices` | GET `/1/indexes` | `listIndexes` |
| `indexes_query` | POST `/1/indexes/{index}/query` | `search` |
| `available_languages` | GET `/1/dictionaries/*/languages` | `settings` |
| `logs` | GET `/1/logs` | `logs` |
| `indexes_settings` | GET `/1/indexes/{index}/settings` | `settings` |

默认读取全部 5 种资源。查询和设置会查找所有可访问的索引，包括日期范围之外的索引。在不输出 `indices` 时仍会查找索引。输出包括 `resource STRING`、`id STRING`、可空的 `parent_id STRING` 和 `record STRING`。ID 分别使用索引名、搜索对象 ID 或日志 SHA-1。语言映射作为一条名为 `languages` 的记录，即使映射为空。索引设置使用同一个索引名作为 ID 和父 ID。查询命中行使用索引名作为父 ID，并保留 API 返回的排名和高亮内容。日志记录会保留供应商负载，其中可能包含请求或响应详情。 [索引列表](https://www.algolia.com/doc/rest-api/search/list-indices) · [语言](https://www.algolia.com/doc/rest-api/search/get-dictionary-languages)

## 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `application_id` | 字符串 | 必填 | Algolia 应用 ID |
| `token` | 字符串 | 必填 | Algolia API 密钥 |
| `start_date` | 字符串 | 必填 | 索引更新时间与日志时间的 UTC 下界，包含此时刻 |
| `base_url` | 字符串 | `https://<application_id>.algolia.net` | 可选的 HTTPS 源地址 |
| `end_date` | 字符串 | 任务开始时 | UTC 扫描上界，不包含此时刻 |
| `resources` | 字符串列表 | 全部 | 资源表中的名称，不可重复 |
| `search_query` | 字符串 | `hitsPerPage=2&getRankingInfo=1` | 作为 JSON `params` 发送的表单编码参数 |
| `indices_page_size` | 整数 | `100` | 每页索引数，1–1000 |
| `logs_page_size` | 整数 | `100` | 每次请求的日志条数，1–1000 |
| `max_pages` | 整数 | `10000` | 每个集合或索引查询的页数上限，1–20000 |
| `request_interval_ms` | 整数 | `25` | 请求之间的最短间隔，10–60000 毫秒 |
| `max_attempts` | 整数 | `4` | 每页最多尝试次数，1–8，含首次请求 |
| `socket_timeout_ms` | 整数 | `15000` | 连接与读取空闲超时，100–120000 毫秒 |
| `page_deadline_ms` | 整数 | `60000` | 每页总耗时上限，含重试，500–600000 毫秒 |
| `max_response_bytes` | 整数 | `8388608` | 响应解压后最大字节数，1024–33554432 |

## 查询范围与恢复

源连接器将 `search_query` 放入搜索请求的 JSON `params` 字段。参数不含 `page` 和 `offset` 时，从第 0 页开始遍历并核对命中数。服务端上限、近似计数或结果页被截断时，任务可能失败。若问题在较后页面才出现，前面的行可能已输出。指定 `page` 时只读一页。指定 `offset` 及可选的 `length` 时只读一个窗口。显式请求仍受服务端可访问范围限制。连接器只读索引设置。 [索引查询](https://www.algolia.com/doc/rest-api/search/search-single-index) · [分页上限](https://www.algolia.com/doc/api-reference/api-parameters/paginationLimitedTo)

索引更新和日志记录在本地按 `start_date` 和截止时间过滤。起点包含，截止时间排除。使用 UTC 时间，例如 `2026-11-27T00:00:00Z` 或 `2026-11-27T00:00:00.125Z`。日志来自配置的源地址，仅涵盖服务仍保留的记录。Algolia 保留最近 7 天的日志，每个服务器最多记录 1,000 个请求。连接器遵守该边界，读取配置源地址的日志。 [日志接口](https://www.algolia.com/doc/rest-api/search/get-logs)

恢复时保留截止时间、资源、父项及页位置。在跳过已处理行之前会验证重新获取的页面。恢复时应用 ID、源地址、资源列表、日期范围、查询参数和每页大小应一致。可以轮换密钥。集合可能在请求之间变化。每次新任务使用指定日期区间，水位不会自动前移。

## 示例

```hocon
env {
  job.mode = "BATCH"
}

source {
  Algolia {
    application_id = "${algolia_application_id}"
    token = "${algolia_token}"
    start_date = "2026-11-27T00:00:00Z"
    resources = ["indices", "indexes_query"]
    search_query = "query=desk&hitsPerPage=100&page=0"
  }
}

sink {
  Console {}
}
```

<ChangeLog />
