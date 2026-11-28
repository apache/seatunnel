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

import ChangeLog from '../changelog/connector-http-aha.md';

# Aha

`Aha` 源连接器通过 [Aha v1 REST API](https://www.aha.io/api) 读取凭据有权访问的 Aha! Roadmaps 与 Ideas 记录。连接器执行有界全量读取，请将 `job.mode` 设为 `BATCH`，并将 `parallelism` 设为 `1`。

## 资源

| 资源 | `/api/v1` 下的路径 | 父级发现 |
| --- | --- | --- |
| `features` | `/features` | — |
| `products` | `/products` | — |
| `idea_categories` | `/products/{product_id}/idea_categories` | 遍历所有产品页 |
| `ideas` | `/ideas` | — |
| `idea_endorsements` | `/ideas/{idea_id}/endorsements` | 遍历所有创意页 |
| `idea_comments` | `/ideas/{idea_id}/idea_comments` | 遍历所有创意页 |
| `users` | `/users` | — |
| `goals` | `/goals` | — |

每个已选子资源都会独立遍历所有可见父级的分页，即使 `resources` 未选择对应父资源。仅当父资源被单独选中时，连接器才输出相应的父级记录。

每行包含四个字符串列。`resource` 是资源名，`record_id` 是服务端记录 ID，顶级资源的 `parent_id` 为空，`data` 为服务端完整对象的 JSON 字符串。连接器保留 JSON 字段名、嵌套值、空值和数值精度。服务端对象可能包含用户信息和创意内容，请在目标端应用相应的数据处理策略。

## 参数

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `api_url` | String | 是 | — | 账户源地址，例如 `https://company.aha.io`。 |
| `token` | String | 是 | — | API 密钥或已签发的 OAuth 2 访问令牌，无需填写 `Bearer` 前缀。 |
| `resources` | List\<String\> | 否 | 全部八个 | 按列表顺序扫描资源，每个资源名只能出现一次。 |
| `page_size` | Integer | 否 | 30 | 每页请求的记录数，范围为 1 至 200。 |
| `max_pages` | Integer | 否 | 10000 | 单个直接资源、父级发现或单个子级集合的页数上限，范围为 1 至 20000。 |
| `max_parents` | Integer | 否 | 10000 | 每个已选子资源允许发现的父级数量上限，范围为 1 至 20000。 |
| `max_response_bytes` | Integer | 否 | 4194304 | 线上响应体和解压后响应体分别应用此字节上限，范围为 1024 至 16777216。 |
| `connect_timeout_ms` | Integer | 否 | 10000 | TCP 建连超时，单位为毫秒，范围为 1 至 300000。 |
| `read_timeout_ms` | Integer | 否 | 10000 | TLS 握手及响应读取空闲超时，单位为毫秒，范围为 1 至 300000。 |
| `request_timeout_ms` | Integer | 否 | 30000 | 每页请求的总时间上限，包含重试和等待，单位为毫秒，范围为 1 至 300000。 |
| `max_attempts` | Integer | 否 | 3 | 每页最多尝试次数，包含首次请求，范围为 1 至 10。 |

使用账户的 HTTPS `tenant.aha.io` 源地址，仅在需要时指定端口。地址中请勿填写 API 路径、凭据、查询字符串或片段。请通过安全参数注入令牌。连接器沿用凭据在该账户中的权限，支持个人 API 密钥或已签发的 OAuth 访问令牌。OAuth 令牌由作业外部续期。

## 分页与恢复

连接器按连续页码构造 v1 请求，并读取响应中的 `current_page`、`total_pages` 和 `total_records`。整页校验通过后才会输出记录。响应被截断、分页元数据不一致、返回重复页面、凭据过期或无权限，以及路由不可访问时，作业均会失败。HTTP 429、500、502、503、504 可在当前页的总时间预算内重试。连接器在预算内遵守服务端限流重置时间与标准重试等待时间。连接器验证 TLS 证书，不跟随重定向或响应中的链接。

恢复时从首个未完成的页继续，账户源地址、资源顺序和每页大小须与检查点一致。恢复后可能重新请求该页。Aha v1 按页码读取，期间 Aha 中的记录可能发生变化。服务端没有为此模式提供不可变快照或增量游标。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Aha {
    api_url = "https://company.aha.io"
    token = "${aha_token}"
    resources = ["products", "idea_categories", "ideas", "idea_comments"]
    page_size = 30
  }
}

sink {
  Console {}
}
```

<ChangeLog />
