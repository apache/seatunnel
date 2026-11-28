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

import ChangeLog from '../changelog/connector-http-apify-dataset.md';

# ApifyDataset

ApifyDataset 从 Apify Storage 读取数据集元数据和条目。使用批处理模式，源端并行度设为 1。连接器通过 HTTPS 使用 Bearer 令牌认证，并按顺序读取所选资源。

## 特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## 资源

| 资源 | 接口 | 行内容 |
| --- | --- | --- |
| `dataset_collection` | `GET /v2/datasets` | 按创建时间返回数据集元数据，每个数据集一行。Apify 默认列出有名称的数据集。 |
| `dataset` | `GET /v2/datasets/{datasetId}` | 所选数据集的一行元数据。 |
| `item_collection_website_content_crawler` | `GET /v2/datasets/{datasetId}/items` | 每行保留一个原始条目对象，包含嵌套的爬取信息和自定义字段。 |
| `item_collection` | `GET /v2/datasets/{datasetId}/items` | 每行输出一个 `{"data": {...}}` 对象，`data` 包含完整条目。 |

两个条目资源读取同一数据集，分别按各自的结构输出。原始 JSON 保留隐藏字段和空条目。数据集可以使用 ID 或 `username~dataset-name` 这类名称引用。 [Apify API](https://docs.apify.com/api/v2)

## 源端选项

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 | — | Apify API Bearer 令牌。 |
| `dataset_id` | String | 按资源 | — | 数据集 ID 或名称引用，读取单个数据集元数据或任何条目资源时必填。 |
| `resources` | 字符串列表 | 否 | 上表所列四个资源及顺序 | 按读取顺序选择资源，名称不能重复。 |
| `api_url` | String | 否 | `https://api.apify.com` | Apify API 的 HTTPS 源地址，只接受 api.apify.com 主机名，可指定端口。 |
| `page_size` | Integer | 否 | `50` | 每页请求的原始扫描行数上限，范围为 1 到 1000。 |
| `max_pages` | Integer | 否 | `10000` | 每个资源的页数上限，范围为 1 到 100000。达到上限时扫描尚未完成则任务失败。 |
| `max_response_bytes` | Integer | 否 | `16777216` | 压缩和解压后的响应体各自的字节上限，范围为 1024 到 33554432。 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时，范围为 1 到 300000。 |
| `read_timeout_ms` | Integer | 否 | `10000` | TLS 和响应读取的空闲超时，范围为 1 到 300000。 |
| `request_timeout_ms` | Integer | 否 | `30000` | 单页请求的总时限，包含重试和等待，范围为 1 到 300000。 |
| `max_attempts` | Integer | 否 | `4` | 单页请求次数上限，包含首次，范围为 1 到 10。 |

## 输出与恢复

表包含三个字符串字段。

| 字段 | 含义 |
| --- | --- |
| `resource` | 上表中的资源名称。 |
| `dataset_id` | 元数据行保存供应商 ID，条目行保存配置中的数据集引用。 |
| `payload` | 资源行的 JSON 文本，保留字段名、嵌套数组、null 及精确 JSON 数值。 |

数据集元数据会去掉顶层 `urlSigningSecretKey` 和 `itemsPublicUrl`，因为这两项可能授予数据集访问权限。业务条目中的同名字段会保留。

每个分页资源使用首页返回的总数作为扫描上限。条目页按供应商实际扫描的行数推进，返回条数少于扫描数或为空时仍可跨过已扫描范围。首页总数之外的追加行留到后续批次。后续响应总量缩小、偏移量重复、扫描停滞或返回条数超过扫描数时，任务会失败。Apify 的行数统计可能短暂滞后于数据写入。需要可重复导出时，应选择已经写入完成的数据集。 [条目接口](https://docs.apify.com/api/v2/dataset-items-get) · [数据集元数据接口](https://docs.apify.com/api/v2/dataset-get)

检查点保存资源、扫描边界和页面偏移。如果某页只输出了部分行，恢复时会重新查询同一逻辑页，验证响应摘要后从保存的位置继续。一个页面或资源完成后，恢复会从下一个待处理页面或资源继续。Apify 上已经读取的其他页面仍可能独立变化。

TLS 使用 JVM 信任库并验证供应商的主机名。重定向、认证或权限错误和数据集不存在会使任务失败。临时 429、500、502、503 和 504 响应可在单页总时限内重试。源端错误和检查点状态不会包含配置中的 API 令牌。

## 示例

把 API 令牌作为 SeaTunnel 作业变量传入。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  ApifyDataset {
    token = "${APIFY_API_TOKEN}"
    dataset_id = "my-account~product-catalog"
    resources = ["dataset", "item_collection"]
    page_size = 50
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
