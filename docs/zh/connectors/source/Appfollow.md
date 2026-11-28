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

import ChangeLog from '../changelog/connector-http-appfollow.md';

# Appfollow

Appfollow 从 AppFollow v2 API 读取账户和应用数据。使用批处理模式，源端并行度设为 1。连接器通过验证主机名的 HTTPS，在 `X-AppFollow-API-Token` 请求头中传入令牌。 [AppFollow 认证文档](https://docs.api.appfollow.io/reference/authorization)

## 特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## 资源

| 资源 | 接口 | 行内容 |
| --- | --- | --- |
| `users` | `GET /api/v2/account/users` | 根响应中的用户记录。 |
| `app_collections` | `GET /api/v2/account/apps` | `apps` 中的应用集合记录。 |
| `app_lists` | `GET /api/v2/account/apps/app?apps_id=…` | `apps_app` 中的应用记录，补充所属集合 ID 和 `app.ext_id` 对应的外部 ID。 |
| `stat_reviews` | `GET /api/v2/reviews/stats?ext_id=…` | 包含 `reviews_stat` 的完整统计响应对象。 |
| `ratings` | `GET /api/v2/meta/ratings?ext_id=…` | 保留 `ratings.list` 的完整评分响应对象。 |

选择应用或指标资源时，连接器自动发现所需的上级资源，只输出所选资源的行。同一个应用出现在两个集合中时，两条关联都会保留。缺少外部 ID 的应用可通过 `app_lists` 读取。选择指标资源时，每个已发现的应用都必须有外部 ID。

以上 v2 接口返回完整响应且没有分页参数。遇到意外续页信号或响应结构不符时，任务会失败。AppFollow 接口权限和额度费用按 API 方法和账户而定，每个发现请求、指标请求和重试都计入本地预算。供应商仍会累计同一令牌在不同作业中的使用量。 [AppFollow 接口费用与权限](https://docs.api.appfollow.io/reference/methods-cost-and-availability)

## 源端选项

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 | — | AppFollow v2 API 令牌。 |
| `resources` | 字符串列表 | 否 | 全部五个资源 | 选择要输出的资源，名称不能重复。连接器自动发现所需的上级资源。 |
| `api_url` | String | 否 | `https://api.appfollow.io` | api.appfollow.io 的 HTTPS 源地址，可指定端口。 |
| `max_entities` | Integer | 否 | `10000` | 扫描中可发现的集合数和应用关联数各自的上限，范围为 1 到 50000。 |
| `max_requests` | Integer | 否 | `1000` | 单个读取器执行期间的请求次数上限，包含发现请求和重试，范围为 1 到 100000。 |
| `max_records` | Integer | 否 | `10000` | 每个响应在输出前可包含的记录数上限，范围为 1 到 100000。 |
| `max_response_bytes` | Integer | 否 | `16777216` | 压缩和解压后的响应体各自的字节上限，范围为 1024 到 33554432。 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时，范围为 1 到 300000。 |
| `read_timeout_ms` | Integer | 否 | `10000` | TLS 和响应读取的空闲超时，范围为 1 到 300000。 |
| `request_timeout_ms` | Integer | 否 | `30000` | 单个请求的总时限，包含重试，范围为 1 到 300000。 |
| `max_attempts` | Integer | 否 | `4` | 单个请求的次数上限，包含首次，范围为 1 到 10。 |

## 输出与恢复

表包含四个字符串字段。

| 字段 | 含义 |
| --- | --- |
| `resource` | 所选资源名称。 |
| `app_collection_id` | 集合、应用和指标行的所属集合，用户行的值为 null。 |
| `ext_id` | 应用及指标行的应用外部 ID，可用时输出。 |
| `data` | 完整 JSON 记录，保留嵌套字段、null 和精确数值。 |

检查点保存已发现的集合和应用 ID、当前资源和父级位置，以及部分输出的响应摘要。恢复时跳过已完成的请求，重新查询待处理的响应。部分响应内容变化时，源端会在恢复保存的位置前报告失败。恢复启动的新读取器会重新计算本地请求预算。AppFollow 上已经读取的其他记录仍可能独立变化。

TLS 使用 JVM 信任库并验证配置中的主机名。重定向、认证、权限、额度、请求无效和所需集成失败都会使作业失败。临时 HTTP 429、500、502、503 和 504 可在请求次数和时限内重试。源端错误和检查点状态不会包含令牌。

## 示例

把令牌作为 SeaTunnel 作业变量传入。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Appfollow {
    token = "${APPFOLLOW_API_TOKEN}"
    resources = ["users", "app_collections"]
    max_requests = 20
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
