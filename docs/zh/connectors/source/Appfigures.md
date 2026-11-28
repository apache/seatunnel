import ChangeLog from '../changelog/connector-http-appfigures.md';

# Appfigures

{/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */}

Appfigures 在有界批作业中读取一个 Appfigures v2 资源，并行度设为 1。在 Appfigures 中为账户创建个人访问令牌，并为令牌所属的 API 客户端授予所需权限。 [个人访问令牌](https://docs.appfigures.com/guides/authentication/personal-access-tokens/)

## 资源

| 资源 | API 路径 | 输出 |
| --- | --- | --- |
| `status` | `/` | `user` 字段中的已认证用户。 |
| `data_categories` | `/data/categories` | 每个分类 ID 对应一行。 |
| `users` | `/users` | 分页读取用户，可按最后登录时间筛选。 |
| `usage` | `/usage` | 一条 API 请求与伙伴 API 额度的使用记录。 |
| `products_mine_search` | `/products/mine` | 账户产品字典或分页结果中的每个产品对应一行。 |
| `reports_sales` | `/reports/sales` | 销售汇总或分组记录。 |
| `reports_revenue` | `/reports/revenue` | 收入汇总或分组记录。 |
| `reports_subscriptions` | `/reports/subscriptions` | 订阅汇总或分组记录。 |
| `reports_ads` | `/reports/ads` | 广告发布汇总或分组记录。 |
| `reports_adspend` | `/reports/adspend` | 广告支出汇总或分组记录。 |
| `reports_ratings` | `/reports/ratings` | 评分汇总或分组记录。 |

读取账户产品需具备 `products:read`。用户列表需要账户读取权限，查看同账户下其他用户还需要管理员角色。销售、收入、订阅、广告和广告支出需具备 `private:read`。评分需具备 `public:read`，可用范围还取决于账户的 Public Data API 订阅。 [用户](https://docs.appfigures.com/reference/users/) · [产品](https://docs.appfigures.com/reference/products/) · [评分](https://docs.appfigures.com/reference/reports-ratings/)

## 输出字段

| 字段 | 类型 | 内容 |
| --- | --- | --- |
| `resource` | String | 当前资源的标准名称。 |
| `record_id` | String | 用户、产品或分类 ID。API 额度使用 `usage`，未分组报表使用 `total`，分组报表使用完整键路径的摘要。 |
| `dimension_keys` | String | 按供应商嵌套顺序排列的 JSON 键数组。字典记录包含其条目键。没有外层键的记录使用 `[]`。 |
| `payload` | String | 保留供应商字段、嵌套值和 null 的 JSON 对象。数值保留精度，供应商以文本返回的金额继续保留为文本。 |

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `resource` | String | 是 | — | 选择上表中的一个资源。 |
| `token` | String | 是 | — | Appfigures 个人访问令牌。 |
| `search_store` | String | 否 | 账户产品为 `apple` | Appfigures 商店标识，例如 `apple`、`google_play`。报表未设置该值时采用 API 默认的产品范围。 |
| `group_by` | String | 否 | 空 | 用逗号分隔的报表分组维度。为空时使用供应商汇总结果。 |
| `report_start_date` | String | 否 | — | 报表起始日期，包含当天，格式为 `YYYY-MM-DD`。指定区间时需同时提供起止日期。 |
| `report_end_date` | String | 否 | — | 报表截止日期，包含当天，格式为 `YYYY-MM-DD`。 |
| `user_start_time` | String | 否 | — | 用户本地 `last_login` 的下界，包含该时间。 |
| `user_end_time` | String | 否 | — | 用户本地 `last_login` 的上界，不包含该时间。 |
| `page_size` | Integer | 否 | `10` | 用户分页大小，可填 1 到 100。账户产品返回分页封套时，后续请求沿用初始响应声明的分页大小。 |
| `max_pages` | Integer | 否 | `1000` | 分页资源的最大页数，可填 1 到 10000。 |
| `max_rows` | Integer | 否 | `100000` | 单次响应或分页资源的记录上限，可填 1 到 500000。 |
| `max_response_bytes` | Integer | 否 | `8388608` | 解码后的响应字节上限，可填 1024 到 67108864。 |
| `max_attempts` | Integer | 否 | `4` | 可重试请求的最大尝试次数，可填 1 到 10。 |
| `request_timeout_ms` | Integer | 否 | `30000` | 包含重试的请求时间预算，可填 100 到 300000 毫秒。 |
| `endpoint` | String | 否 | `https://api.appfigures.com` | HTTPS 端点。自定义地址需同时配置 `trust_certificate`。连接器自动添加 v2 路径。 |
| `trust_certificate` | String | 否 | — | 自定义端点的 Worker 本地 PEM 信任证书。连接器仍校验端点主机名。 |

## 报表与登录时间范围

- 六类报表均支持 `product`、`country` 和 `date`。销售、收入、订阅、广告、广告支出支持 `store`。广告与广告支出支持 `network`，销售支持 `device`。语义相同的复数维度名也可使用。报表行保留原有分组键路径和供应商叶节点的字段。 [销售](https://docs.appfigures.com/reference/sales/) · [收入](https://docs.appfigures.com/reference/revenue/) · [订阅](https://docs.appfigures.com/reference/subscriptions/) · [广告](https://docs.appfigures.com/reference/ads/) · [广告支出](https://docs.appfigures.com/reference/ad-spend/)
- 使用 `date` 维度且未指定区间时，连接器在作业开始时固定最近 30 个完整的 UTC 日历日。其他不带日期的查询采用 Appfigures 默认范围。显式销售区间最多包含 30 天。Appfigures 返回的历史汇总可能与逐日结果的加总不同。
- 报表配置 `search_store` 后，连接器先从账户产品接口取得该商店的有效产品 ID，再查询报表，因此也需要 `products:read`。该商店没有匹配产品时返回空结果。单次请求无法容纳全部产品 ID 时，连接器会明确失败，使报表的汇总和比率由供应商一次计算。
- 用户登录筛选按 Appfigures 返回的本地时间值比较，例如 `2025-11-01T12:30:00`。请使用适用于这些本地时间值的边界。原始时间和时区字段保留在 payload 中。未筛选时也读取尚未登录的用户。

## 恢复与失败处理

Zeta 检查点保存当前页或报表产品范围，以及部分输出响应的摘要和行位置。恢复时发现部分响应改变、读取配置变化或分页不一致，作业会失败。Appfigures 的账户记录可能在读取期间被修改，恢复较旧检查点时可能重放记录。

权限被拒绝、Public Data 额度耗尽、响应格式错误或超出每日额度时，连接器明确报错。HTTP 420 表示达到每日 API 上限。临时 HTTP 429 和服务器错误会在请求时限内有限重试。HTTPS 请求限定在配置的端点。故障诊断省略凭证和响应正文。 [速率限制](https://docs.appfigures.com/rate-limits/)

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Appfigures {
    resource = "reports_sales"
    token = ${APPFIGURES_TOKEN}
    search_store = "apple"
    group_by = "product,country"
    report_start_date = "2026-01-01"
    report_end_date = "2026-01-30"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
