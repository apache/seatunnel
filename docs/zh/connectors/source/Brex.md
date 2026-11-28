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

import ChangeLog from "../changelog/connector-http-brex.md";

# Brex

## 描述

Brex Source 以有界批作业读取六种资源。连接器使用控制台签发的用户令牌，向 [Brex API](https://developer.brex.com/) 发送 HTTPS GET 请求。请为令牌授予已选资源的读取权限。可见记录取决于账户、令牌权限和用户角色。[创建用户令牌](https://developer.brex.com/guides/authentication)、[角色与权限](https://developer.brex.com/guides/roles_permissions_scopes)。

| 资源 | 请求路径 | 建议的读取权限 | 记录主键 |
| --- | --- | --- | --- |
| `transactions` | `/v2/transactions/card/primary` | `transactions.card.readonly` | `id` |
| `users` | `/v2/users` | `users.readonly` | `id` |
| `departments` | `/v2/departments` | `departments.readonly` | `id` |
| `vendors` | `/v1/vendors` | `vendors.readonly` | `id` |
| `expenses` | `/v1/expenses`，带 `expense_type[]=CARD` | `expenses.card.readonly` | `id` |
| `budgets` | `/v2/budgets` | `budgets.readonly` | `budget_id` |

单个 Source 子任务按表格顺序读取所选资源。省略 `resources` 时读取全部六种资源。`users` 默认沿用 API 的状态可见范围。若令牌有权限并且需要读取已归档的用户，在 `user_statuses` 中显式指定 `ARCHIVED`。`transactions` 读取已结算的主卡交易，`expenses` 通过统一费用接口读取卡费用。

## 选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| token | string | 是 | | Brex 控制台用户令牌。 |
| resources | list | 否 | [] | 需要读取的资源名，空列表选择全部六种资源。 |
| user_statuses | list | 否 | [] | `users` 的状态筛选，支持 `INVITED`、`ACTIVE`、`DISABLED`、`PENDING_ACTIVATION`、`INACTIVE`、`ARCHIVED`。 |
| start_date | string | 读取交易或费用时必填 | | 起始时间，使用带时区的 RFC 3339 格式。 |
| end_date | string | 否 | 批作业启动时刻 | 包含该时刻的 RFC 3339 截止时间，首次请求前固定。 |
| transactions_resume_date | string | 否 | | 新批作业使用的上次处理成功的 UTC 入账日期，格式为 `YYYY-MM-DD`。 |
| expenses_resume_at | string | 否 | | 新批作业使用的上次处理成功的购买时间，采用 RFC 3339 格式。 |
| tls_trust_certificate | string | 否 | JVM 默认信任配置 | 仅此 Source 使用的 PEM 或 Base64 DER X.509 信任锚，仍校验服务器主机名。 |
| page_size | int | 否 | 100 | 每页请求 1 至 1,000 项，卡费用最多请求 100 项。 |
| max_pages_per_resource | int | 否 | 10000 | 每种资源最多接受的页数，包含带续页游标的空页。 |
| connect_timeout_ms | int | 否 | 10000 | TCP 连接超时，单位为毫秒。 |
| request_timeout_ms | int | 否 | 30000 | 单次 HTTPS 请求的截止时间，单位为毫秒。 |
| operation_timeout_ms | int | 否 | 60000 | 一次逻辑读取的总时限，包含重试及等待，必须大于或等于 `request_timeout_ms`。 |
| request_interval_ms | int | 否 | 65 | 相邻请求尝试的最短间隔，单位为毫秒。 |
| max_retries | int | 否 | 3 | 符合重试条件时的额外尝试次数。 |
| max_retry_delay_ms | int | 否 | 30000 | 最大重试等待时间。若 `Retry-After` 超过此值，读取失败。 |
| max_response_bytes | int | 否 | 8388608 | 解压前、解压后各自的响应字节上限。 |

空页或未填满的页只要含有 `next_cursor` 就会继续读取。若 `next_cursor` 缺省或为 null，则完成当前资源。游标无效或重复、返回项超过页大小、JSON 无效、重定向、认证或权限错误都会导致读取失败。暂时性传输错误和 HTTP 408、429、500、502、503、504 可在配置的时限内重试。[Brex 分页说明](https://developer.brex.com/guides/pagination)、[限流说明](https://developer.brex.com/guides/rate_limits)。

## 时间窗口与恢复

交易查询发送 `posted_at_start`，并按截止时间对应的 UTC 日期筛选返回的 `posted_at_date`。与截止时间同一天的交易仍可入选，即使截止时间早于该日结束。Source 遍历至最后一个游标。新批作业指定 `transactions_resume_date` 时，从该入账日之前一个 UTC 日开始回读，且下界不会早于 `start_date`。Brex 返回已结算交易可能存在延迟。[Brex API 更新日志](https://developer.brex.com/changelog)。

卡费用按精确时间戳比较，`purchased_at_start` 和 `purchased_at_end` 都包含边界。新批作业指定 `expenses_resume_at` 时，从该时间之前一个 UTC 日历月开始回读，且下界不会早于 `start_date`。回读可能包含上一批已提取的记录，可通过资源名和记录 ID 识别批次间的重复。

检查点保留已固定的截止时间、资源位置、分页游标、当前页摘要及已发出的行数。恢复时重新读取当前页，摘要一致后才输出剩余行。已完成的资源不会重新读取。若游标过期或页面内容已改变，可能需要新启批作业。Brex 集合在读取期间可能发生变化，结果以扫描期间实际返回的数据为准。检查点绑定原用户令牌摘要，更换令牌时需新启批作业，并提供上次成功处理的交易日期和费用时间。

## 输出

Source 输出四个字符串列。

| 列 | 含义 |
| --- | --- |
| `resource` | 资源名。 |
| `id` | 服务返回的原始记录主键，预算使用 `budget_id`。 |
| `cursor` | 交易为 UTC 入账日期，费用为 UTC 购买时间，其他资源为 null。 |
| `record` | 原生记录的 JSON 字符串，保留嵌套结构、null 和其他字段。 |

金额保留 Brex 返回的原币种和带符号的最小货币单位，并保持整数与小数精度。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Brex {
    token = ${BREX_USER_TOKEN}
    resources = ["transactions", "expenses", "budgets"]
    start_date = "2025-01-01T00:00:00Z"
  }
}
```

## 变更日志

<ChangeLog />
