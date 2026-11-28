import ChangeLog from '../changelog/connector-http-amazon-seller-partner.md';

# AmazonSellerPartner

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

AmazonSellerPartner 从亚马逊销售伙伴 API（SP-API）批量读取数据。每个 Source 选择一个资源和一个站点，作业并行度设为 1。连接器直接读取 API 资源。对于允许按需生成的报表，无法确认已有报表符合条件时会发起生成请求。

## 输出字段

| 字段 | 类型 | 内容 |
| --- | --- | --- |
| `resource` | String | 资源的标准名称。 |
| `record_id` | String | 供应商标识或组合标识。其他报表行用报表 ID 和行序号标识，财务事件页用摘要标识。 |
| `parent_id` | String | 子资源对应的订单或入库货件 ID，其他资源为 null。 |
| `marketplace_id` | String | 请求使用的站点 ID。账户级结算报表行的此字段为 null，报表行自带的站点字段保留在 payload 中。 |
| `payload` | String | 包含供应商字段的 UTF-8 JSON 记录。 |

直接 API 返回的 JSON 保留嵌套值、null 和小数精度。财务事件按页保留分组事件数组。子记录包含所属订单或货件 ID，订单明细还包含订单更新时间。JSON 报表读取对应资源的行数组。表格报表用表头映射字段，保留引号内的制表符和跨行文本，并用精确 JSON 数值输出已识别的数值字段。已识别的空数值和空结算时间转为 null。连接器规范化本地化卖家反馈、商品清单、台账和结算日期。XML 报表逐条输出订单或目录节点，重复元素保留为数组。

## 支持的资源

| 资源 | 账号类型 | 记录 |
| --- | --- | --- |
| `Orders` | Seller | API JSON |
| `OrderItems` | Seller | API JSON |
| `ListFinancialEventGroups` | Seller | API JSON |
| `ListFinancialEvents` | Seller | API JSON |
| `FbaInboundShipments` | Seller | API JSON |
| `FbaInboundShipmentItems` | Seller | API JSON |
| `VendorOrders` | Vendor | API JSON |
| `VendorOrdersStatus` | Vendor | API JSON |
| `VendorDirectFulfillmentShipping` | Vendor | API JSON |
| `GET_VENDOR_FORECASTING_FRESH_REPORT` | Vendor | JSON 报表 |
| `GET_VENDOR_FORECASTING_RETAIL_REPORT` | Vendor | JSON 报表 |
| `GET_VENDOR_SALES_REPORT` | Vendor | JSON 报表 |
| `GET_VENDOR_INVENTORY_REPORT` | Vendor | JSON 报表 |
| `GET_VENDOR_TRAFFIC_REPORT` | Vendor | JSON 报表 |
| `GET_VENDOR_NET_PURE_PRODUCT_MARGIN_REPORT` | Vendor | JSON 报表 |
| `GET_VENDOR_REAL_TIME_INVENTORY_REPORT` | Vendor | JSON 报表 |
| `GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT` | Seller | JSON 报表 |
| `GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT` | Seller | JSON 报表 |
| `GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT` | Seller | JSON 报表 |
| `GET_BRAND_ANALYTICS_ALTERNATE_PURCHASE_REPORT` | Seller | JSON 报表 |
| `GET_BRAND_ANALYTICS_ITEM_COMPARISON_REPORT` | Seller | JSON 报表 |
| `GET_SALES_AND_TRAFFIC_REPORT` | Seller | JSON 报表 |
| `GET_SALES_AND_TRAFFIC_REPORT_BY_DATE` | Seller | JSON 报表 |
| `GET_SALES_AND_TRAFFIC_REPORT_BY_MONTH` | Seller | JSON 报表 |
| `GET_ORDER_REPORT_DATA_SHIPPING` | Seller | XML 报表 |
| `GET_XML_BROWSE_TREE_DATA` | Seller | XML 报表 |
| `GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL` | Seller | XML 报表 |
| `GET_SELLER_FEEDBACK_DATA` | Seller | 本地化表格报表 |
| `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL` | Seller | 制表符分隔报表 |
| `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL` | Seller | 制表符分隔报表 |
| `GET_FBA_STORAGE_FEE_CHARGES_DATA` | Seller | 制表符分隔报表 |
| `GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL` | Seller | 制表符分隔报表 |
| `GET_FLAT_FILE_OPEN_LISTINGS_DATA` | Seller | 制表符分隔报表 |
| `GET_FBA_FULFILLMENT_REMOVAL_ORDER_DETAIL_DATA` | Seller | 制表符分隔报表 |
| `GET_FLAT_FILE_ACTIONABLE_ORDER_DATA_SHIPPING` | Seller | 制表符分隔报表 |
| `GET_LEDGER_DETAIL_VIEW_DATA` | Seller | 制表符分隔报表 |
| `GET_LEDGER_SUMMARY_VIEW_DATA` | Seller | 制表符分隔报表 |
| `GET_FBA_REIMBURSEMENTS_DATA` | Seller | 制表符分隔报表 |
| `GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA` | Seller | 制表符分隔报表 |
| `GET_AFN_INVENTORY_DATA` | Seller | 制表符分隔报表 |
| `GET_AFN_INVENTORY_DATA_BY_COUNTRY` | Seller | 制表符分隔报表 |
| `GET_FBA_INVENTORY_PLANNING_DATA` | Seller | 制表符分隔报表 |
| `GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE` | Seller | 制表符分隔报表 |
| `GET_FLAT_FILE_ARCHIVED_ORDERS_DATA_BY_ORDER_DATE` | Seller | 制表符分隔报表 |
| `GET_MERCHANTS_LISTINGS_FYP_REPORT` | Seller | 制表符分隔报表 |
| `GET_MERCHANT_CANCELLED_LISTINGS_DATA` | Seller | 制表符分隔报表 |
| `GET_MERCHANT_LISTINGS_DATA_BACK_COMPAT` | Seller | 制表符分隔报表 |
| `GET_STRANDED_INVENTORY_UI_DATA` | Seller | 制表符分隔报表 |
| `GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA` | Seller | 制表符分隔报表 |
| `GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_PROMOTION_DATA` | Seller | 制表符分隔报表 |
| `GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA` | Seller | 制表符分隔报表 |
| `GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT` | Seller | 制表符分隔报表 |
| `GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_REPLACEMENT_DATA` | Seller | 制表符分隔报表 |
| `GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA` | Seller | 制表符分隔报表 |
| `GET_MERCHANT_LISTINGS_ALL_DATA` | Seller | 制表符分隔报表 |
| `GET_MERCHANT_LISTINGS_DATA` | Seller | 制表符分隔报表 |
| `GET_MERCHANT_LISTINGS_INACTIVE_DATA` | Seller | 制表符分隔报表 |
| `GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2` | Seller | 制表符分隔报表 |

`Orders` 和 `OrderItems` 使用 Orders v0，保留其数据和受限令牌语义。亚马逊公布的相关 Orders v0 操作停用日期为 2027 年 3 月 27 日。 [SP-API 弃用公告](https://developer-docs.amazon/sp-api/docs/sp-api-deprecations)

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `resource` | String | 是 | — | 选择上表中的资源名。 |
| `account_type` | String | 否 | `SELLER` | 选择 SELLER 或 VENDOR，与资源所属的账号类型一致。 |
| `environment` | String | 否 | `PRODUCTION` | 选择 PRODUCTION 或 SANDBOX。 |
| `region` | String | 否 | `US` | 站点国家或地区代码。 |
| `client_id` | String | 是 | — | Login with Amazon OAuth 客户端 ID。 |
| `client_secret` | String | 是 | — | Login with Amazon OAuth 客户端密钥。 |
| `token` | String | 是 | — | 销售伙伴刷新令牌。 |
| `include_pii` | Boolean | 否 | `false` | 为支持的资源启用相应操作范围的受限访问。 |
| `start_time` | String | 否 | — | 通用读取区间的起点，包含该时间。 |
| `end_time` | String | 否 | — | 通用读取区间的排他截止时间，受供应商可用范围限制。 |
| `period_in_days` | Integer | 否 | `90` | 通用切片天数上限，可填 1 到 365。部分资源的上限更低。 |
| `financial_events_step` | String | 否 | `180D` | 财务切片时长，格式为正整数加 H 或 D，最长 180 天。 |
| `financial_page_size` | Integer | 否 | `100` | 财务 API 分页大小，可填 1 到 100。 |
| `inbound_replication_mode` | String | 否 | `ROLLING` | FBA 入库选择 ROLLING 或 FIXED。 |
| `inbound_rolling_days` | Integer | 否 | `30` | FBA 入库滚动回溯天数，可填 1 到 730。 |
| `inbound_start_datetime` | String | 否 | — | FBA 入库起点，包含该时间。FIXED 模式必填。 |
| `inbound_end_datetime` | String | 否 | — | FIXED 模式下 FBA 入库排他截止时间。省略时使用作业启动时间。 |
| `report_options_list` | String | 否 | `[]` | 各报表视图的选项，格式为 JSON 数组字符串。 |
| `asin_granularity` | String | 否 | `PARENT` | 按 ASIN 读取销售和流量时选择 PARENT、CHILD 或 SKU。 |
| `report_lookback_hours` | Integer | 否 | `0` | 将显式指定的报表起点前移指定小时数，可填 0 到 720。 |
| `max_done_report_age_hours` | Integer | 否 | `0` | 匹配已有完成报表时允许的最大报表年龄，可填 0 到 72 小时。0 表示不复用已完成报表。 |
| `treat_cancelled_as_empty` | Boolean | 否 | `false` | 交由本次读取创建的报表如果被取消且没有文档，允许按空结果处理。 |
| `max_requests` | Integer | 否 | `10000` | 每个分片的供应商操作和报表输出批次总上限，可填 1 到 100000。 |
| `max_attempts` | Integer | 否 | `4` | 可重试操作的 HTTP 最大尝试次数，可填 1 到 10。 |
| `max_rate_limit_retries` | Integer | 否 | `3` | 每个操作遇到 HTTP 429 时的最大重试次数，可填 0 到 9。 |
| `stop_sync_on_rate_limit` | Boolean | 否 | `false` | 报表请求首次收到 HTTP 429 时立即失败。 |
| `request_timeout_ms` | Integer | 否 | `30000` | 包含重试的单次请求预算，可填 100 到 300000 毫秒。 |
| `report_timeout_ms` | Integer | 否 | `3600000` | 等待单个报表的时间上限，可填 1000 到 14400000 毫秒。 |
| `poll_interval_ms` | Integer | 否 | `30000` | 报表状态轮询间隔，可填 100 到 300000 毫秒。 |
| `max_response_bytes` | Integer | 否 | `8388608` | 响应或解码后文档的字节上限，可填 1024 到 67108864。 |
| `max_report_rows` | Integer | 否 | `100000` | 单个报表的解码行数上限，可填 1 到 500000。 |
| `sandbox_origin` | String | 否 | — | 自定义 SANDBOX HTTPS 端点。需同时提供 trust_certificate。 |
| `trust_certificate` | String | 否 | — | 自定义沙箱的 Worker 本地 PEM 信任证书。 |

支持的站点代码为 `AE`、`AU`、`BE`、`BR`、`CA`、`DE`、`EG`、`ES`、`FR`、`GB`（也可用 `UK`）、`IE`、`IN`、`IT`、`JP`、`MX`、`NL`、`PL`、`SA`、`SE`、`SG`、`TR`、`US`。连接器按代码选择区域端点和站点 ID。 [SP-API 区域端点](https://developer-docs.amazon/sp-api/docs/sp-api-endpoints)

## 时间范围和报表参数

- 日期参数使用带时区偏移、精确到秒的 RFC 3339 时间。连接器转为 UTC，并在作业开始时固定读取截止时间。订单和财务 API 预留最近两分钟。FBA 入库资源使用独立的固定或滚动区间。一般资源的默认回溯上限为 730 天，实际可用历史取决于资源。
- 日报与月报仅读取选择范围内完整的 UTC 日历周期。Vendor 日报默认预留 72 小时发布时差，亚马逊 Vendor 站点指标遵循供应商的太平洋时间报表日历。品牌分析报表读取截止日期前最近一个完整的周日到周六区间。Vendor Forecasting 按销售项目读取最新快照。 [分析报表](https://developer-docs.amazon/sp-api/docs/report-type-values-analytics)
- `report_options_list` 是一个 JSON 数组字符串。每项包含唯一的 `stream_name` 和一个 `options_list`，列表中的 `option_name` 必须唯一，`option_value` 是字符串。连接器只把对应选项用于当前资源。品牌分析固定 `reportPeriod=WEEK`，预测视图分别固定 `FRESH` 或 `RETAIL`，销售和流量视图固定 `dateGranularity=DAY` 或 `MONTH`。
- Vendor 的销售、库存、流量、净产品毛利视图固定使用 `reportPeriod=DAY`。Vendor 销售和库存还必须提供 `distributorView`（`MANUFACTURING` 或 `SOURCING`）以及 `sellingProgram`（两者可用 `RETAIL` 或 `FRESH`，销售还可用 `BUSINESS`）。Vendor 实时库存按日请求，保留供应商返回的小时数据。 [Vendor 零售分析报表](https://developer-docs.amazon/sp-api/docs/report-type-values-analytics#vendor-retail-analytics-reports)
- 定时生成的 XML 发货报表 `GET_ORDER_REPORT_DATA_SHIPPING` 和自动生成的结算报表按创建时间读取账户已有报表，范围限制为最近 89 天。请在亚马逊侧预先配置发货报表计划，读取时设置 `include_pii = true`，并确保亚马逊返回的报表明确只属于所选站点。`GET_FLAT_FILE_ACTIONABLE_ORDER_DATA_SHIPPING` 支持按需生成，同样需要受限权限。 [订单报表](https://developer-docs.amazon/sp-api/docs/report-type-values-order)

## 认证、报表读取与恢复

使用已授权的 Login with Amazon 客户端和销售伙伴刷新令牌。连接器通过区域 HTTPS 端点并发送 `x-amz-access-token`。设置 `include_pii = true` 后，订单、订单明细、Vendor 发货标签和受限报表文档请求使用对应操作范围的 Restricted Data Token（RDT）。Vendor 发货标签保留 90 天，读取时必须设置 `include_pii = true`。权限被拒绝时作业失败。使用该选项前需为销售伙伴启用相应角色，并确保目标端获准存储受限数据。自定义沙箱需要配置 Worker 本地证书，并执行主机名校验。报表通过供应商返回的 S3 或 CloudFront HTTPS 地址下载，下载不携带 API 认证头，也不会跟随重定向。 [Tokens API](https://developer-docs.amazon/sp-api/docs/tokens-api-use-case-guide) · [下载报表](https://developer-docs.amazon/sp-api/docs/retrieve-a-report)

复用外部报表要求类型、站点、时间范围和明确返回的选项值全部匹配。亚马逊常规报表元数据省略选项，此时无法确认等价。检查点中属于当前作业的报表 ID 可以直接恢复。`max_done_report_age_hours` 还限制已完成报表的可复用时长。创建结果不确定且无法确认已有匹配报表时，作业明确失败。恢复已保存的创建意图后会先查证已有报表。`FATAL`、未知状态及未确认空结果的 `CANCELLED` 均导致失败。文档链接到期后可在请求时限内通过元数据刷新。

Zeta 检查点记录当前页、父记录、时间切片、报表 ID、处理阶段和文档行位置。部分报表已输出时，恢复后重新计算文档摘要，内容变化会导致失败。供应商游标可能过期，外部数据可能变化，恢复较旧的检查点可能重放记录或重新发起较早的报表请求。需要去重时，可用记录标识和业务字段在下游处理。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  AmazonSellerPartner {
    resource = "Orders"
    region = "US"
    client_id = ${SP_CLIENT_ID}
    client_secret = ${SP_CLIENT_SECRET}
    token = ${SP_REFRESH_TOKEN}
    start_time = "2026-01-01T00:00:00Z"
    end_time = "2026-01-02T00:00:00Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
