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

import ChangeLog from '../changelog/connector-http-bunny-inc.md';

# BunnyInc

安装 `connector-http-bunny-inc` 后，可从 Bunny, Inc. 订阅计费工作区导出数据。为 API 客户端生成具有对应数据读取权限的访问令牌。每个源配置块选择一个资源，使用批模式和源并行度 `1`。[Bunny API 与访问令牌](https://docs.bunny.com/developer/using-the-graphql-api/api)

## 资源

| `resource` | GraphQL 集合 | 记录 |
| --- | --- | --- |
| `plans` | `plans` | 产品套餐 |
| `quotes` | `quotes` | 报价 |
| `tenants` | `tenants` | 租户记录 |
| `accounts` | `accounts` | 计费账户 |
| `contacts` | `contacts` | 账户联系人 |
| `entities` | `entities` | 计费主体 |
| `invoices` | `invoices` | 已开具发票 |
| `payments` | `payments` | 付款记录 |
| `products` | `products` | 产品 |
| `invoiceItems` | `invoiceItems` | 发票明细 |
| `transactions` | `transactions` | 计费交易 |
| `quote_charges` | `quoteCharges` | 报价费用明细 |
| `subscriptions` | `subscriptions` | 订阅 |
| `accountBalances` | `accountBalances` | 账户余额 |
| `subscriptionCharges` | `subscriptionCharges` | 订阅费用明细 |

连接器读取对应的计费字段，包括金额、收费数量、时间戳和空值。订阅字段 `cancellationDate` 在输出中使用 `cancelationDate`，兼容已有导出字段名。[Bunny GraphQL 查询参考](https://docs.bunny.com/developer/api-reference/queries)

## 配置

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `subdomain` | string | 必填 | `.bunny.com` 前的单个 DNS 标签，例如 `my-workspace`。连接器将标签转换为小写后构建 `https://<subdomain>.bunny.com/graphql`。 |
| `token` | string | 必填 | 所选工作区的 Bunny 访问令牌。 |
| `resource` | string | 必填 | 资源表中的一个导出名称。 |
| `page_size` | int | `10` | 每个游标页请求的记录数，范围 1–10。 |
| `max_pages` | int | `2000` | 允许请求的最大游标页数，包括空页。范围 1–20000。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接超时，单位毫秒。范围 1–120000。 |
| `read_timeout_ms` | int | `30000` | 套接字读取超时，单位毫秒。范围 1–120000。 |
| `request_deadline_ms` | int | `60000` | 单次请求及重试的总时限，单位毫秒。范围 1–300000。 |
| `max_attempts` | int | `4` | 可重试响应的最大尝试次数，包含第一次。范围 1–10。 |
| `max_response_bytes` | int | `8388608` | 编码前后各自允许的最大响应字节数。范围 1024–33554432。 |
| `tls_trust_certificate` | string | JVM 信任库 | 可选的引擎本地 PEM 或 DER 文件，提供 X.509 信任证书。连接器仍会校验工作区主机名。 |

## 输出与恢复

| 字符串字段 | 值 |
| --- | --- |
| `entity_id` | Bunny 对象的原始标识转换为字符串，缺失时为 null。 |
| `data` | 完整节点 JSON，保留嵌套字段、数值及小数位数。 |

连接器使用 HTTPS POST 执行只读 GraphQL 查询，完整遍历所选集合。分页依据 `pageInfo.hasNextPage`，并将原始 `endCursor` 放入 JSON 变量。空页也可能提供有效的下一页游标。缺失或重复游标会使任务明确失败。Bearer 令牌只会发往所选主机。连接器拒绝 HTTP 重定向和含 GraphQL 错误的响应，包括同时返回部分数据和错误的 HTTP 200 响应。重试等待遵循 `Retry-After`，且受总时限约束。

检查点保存当前游标、有限数量的游标摘要和部分输出页的行偏移量。恢复时重新读取该页，确认记录顺序、内容和下一页游标一致后继续。分片状态不保存访问令牌或计费记录。服务可能随计费活动变动，接口不保证不可变快照。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  BunnyInc {
    subdomain = "my-workspace"
    token = ${BUNNY_ACCESS_TOKEN}
    resource = "invoiceItems"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
