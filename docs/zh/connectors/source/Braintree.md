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

import ChangeLog from "../changelog/connector-http-braintree.md";

# Braintree

> PayPal Braintree Source 连接器

## 描述

以有界批作业读取 Braintree 的 Production 或 Sandbox 商户数据。填写网关商户 ID 及具备相应资源访问权限的 API 密钥对。连接器使用 Braintree 网关 v6 XML 接口，查询型 POST 用于获取 ID 或记录。

## 资源

| 资源 | 读取方式 | 游标字段 |
| --- | --- | --- |
| `merchant_accounts` | 网关分页 | |
| `customers` | 搜索 ID，再分批查询记录 | `created_at` |
| `discounts` | 单次目录响应 | |
| `plans` | 单次目录响应 | |
| `transactions` | 搜索 ID，再分批查询记录 | `created_at` |
| `subscriptions` | 搜索 ID，再分批查询记录 | `created_at` |
| `disputes` | 网关直接分页搜索 | `received_date` |

可按名称指定资源，省略时读取全部七类。一个实际 Reader 按配置顺序读取。交易搜索上限为 50,000 个 ID，其余搜索上限为 10,000 条。达到上限时，连接器先拆分时间区间再读取记录。单个分钟或争议日期内仍达到上限时，作业报告搜索结果可能不完整。权限和保留期受 Braintree 接口约束。[Braintree 搜索结果与限制](https://developer.paypal.com/braintree/docs/reference/general/searching/search-results/java)。

客户、交易及订阅使用包含两端的 RFC3339 时间。Braintree 搜索会包括上界时间所在的整分钟。连接器请求互不重叠的 UTC 分钟区间，再按配置的精确起止时间过滤。争议的接收日期精度为天，采用包含两端的 UTC 日期。[Braintree 搜索时间语义](https://developer.paypal.com/braintree/docs/reference/general/searching/search-fields/java/)。

## 配置项

| 名称 | 类型 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| environment | string | 是 |  | Production 或 Sandbox。 |
| merchant_id | string | 是 |  | 网关商户标识。 |
| access_key | string | 是 |  | Braintree 公钥。 |
| secret_key | string | 是 |  | Braintree 私钥。 |
| start_date | string | 否 |  | 包含在内的 RFC3339 起始时间。选择搜索型资源时必填。 |
| end_date | string | 否 | 作业开始 | 包含在内的 RFC3339 截止时间。默认冻结作业开始时刻。 |
| tls_trust_certificate | string | 否 | JVM 默认 | 仅用于此 Source 的可选 PEM 或 base64 DER X.509 信任锚。 |
| resources | list | 否 | [] | 选择输出的资源。空列表选择全部七类。 |
| connect_timeout_ms | int | 否 | 10000 | TCP 连接超时。 |
| request_timeout_ms | int | 否 | 30000 | 一次 HTTPS 尝试的期限。 |
| operation_timeout_ms | int | 否 | 60000 | 含重试的整次逻辑读取期限。 |
| max_retries | int | 否 | 3 | 可重试读取的次数上限。 |
| max_retry_delay_ms | int | 否 | 30000 | 允许的 Retry-After 或退避时长上限。 |
| max_response_bytes | int | 否 | 8388608 | XML 解压前后的字节上限。 |
| max_xml_depth | int | 否 | 64 | XML 元素最大嵌套深度。 |
| max_xml_nodes | int | 否 | 500000 | XML 元素总数上限。 |
| max_pages | int | 否 | 10000 | 单资源页数或 ID 批次数上限。 |
| max_search_windows | int | 否 | 10000 | 单搜索资源的区间总数上限，含拆分后区间。 |
| search_window_days | int | 否 | 31 | 每个初始搜索区间的天数。 |
| search_result_limit | int | 否 | 50000 | 达到此数量或更低的供应商上限时拆分搜索。 |
| max_ids_per_request | int | 否 | 1000 | 每批最多请求的 ID 数。 |

## 输出与恢复

四列均为字符串。`resource` 为资源名，`id` 保留原始标识符，`cursor` 是搜索资源的 UTC 时间或日期，目录资源为 null。`record` 是 JSON 对象字符串。

采用下划线字段名，保留 XML 的嵌套对象、数组、空值、布尔值、原始标识及精确小数。已识别的日期时间按 ISO 格式输出。商户、交易及客户详情采用相应的 SDK 风格字段，如 `business_details`、`billing_details` 及 `payment_methods`。保留记录中出现的其它字段。XML 有字节、深度和节点数限制，禁用文档类型和外部实体解析。

检查点保留冻结的时间上界、已完成资源、当前页或已规划的 ID 批次及消费行位置。恢复时会验证已见响应的内容摘要后再继续输出。搜索返回 ID 后，Braintree 可能省略已经删除或不再符合条件的记录。连接器跳过缺失的 ID，并继续剩余批次。读取途中上游集合可能变化。独立批作业需显式配置下一次的日期范围。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Braintree {
    environment = "Sandbox"
    merchant_id = "your-merchant"
    access_key = ${BRAINTREE_PUBLIC_KEY}
    secret_key = ${BRAINTREE_PRIVATE_KEY}
    resources = ["plans", "transactions", "disputes"]
    start_date = "2025-01-01T00:00:00Z"
  }
}
```

## 变更日志

<ChangeLog />
