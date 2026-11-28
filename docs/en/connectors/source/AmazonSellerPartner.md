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

AmazonSellerPartner is a bounded batch source for the Amazon Selling Partner API (SP-API). Each source selects one resource and one marketplace. Set job parallelism to 1. Direct resources use read operations. Reports that support requests are generated when an existing matching report cannot be established.

## Records

| Column | Type | Contents |
| --- | --- | --- |
| `resource` | String | Canonical resource name. |
| `record_id` | String | Provider or composite identifier when available. Other report rows use a report-scoped ordinal, and financial-event pages use a digest. |
| `parent_id` | String | Order or inbound shipment ID for child resources, otherwise null. |
| `marketplace_id` | String | The configured marketplace used for the request. Null for account-level settlement rows, whose own marketplace fields are preserved in the payload. |
| `payload` | String | A UTF-8 JSON record containing the provider fields. |

API JSON preserves nested values, nulls and decimal precision. Financial events preserve each page's grouped event arrays. Child rows include their parent ID and, for order items, the parent's update time. JSON reports select their resource-specific row array. Tabular reports map headers to fields, preserve quoted tabs and multiline values, and emit recognized numeric fields as exact JSON numbers. Empty recognized numeric or settlement timestamp fields become null. The source normalizes localized seller feedback, listing and ledger dates, and settlement timestamps. XML reports emit individual order and browse nodes while retaining repeated elements as arrays.

## Supported resources

| Resource | Account | Records |
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
| `GET_VENDOR_FORECASTING_FRESH_REPORT` | Vendor | JSON report |
| `GET_VENDOR_FORECASTING_RETAIL_REPORT` | Vendor | JSON report |
| `GET_VENDOR_SALES_REPORT` | Vendor | JSON report |
| `GET_VENDOR_INVENTORY_REPORT` | Vendor | JSON report |
| `GET_VENDOR_TRAFFIC_REPORT` | Vendor | JSON report |
| `GET_VENDOR_NET_PURE_PRODUCT_MARGIN_REPORT` | Vendor | JSON report |
| `GET_VENDOR_REAL_TIME_INVENTORY_REPORT` | Vendor | JSON report |
| `GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT` | Seller | JSON report |
| `GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT` | Seller | JSON report |
| `GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT` | Seller | JSON report |
| `GET_BRAND_ANALYTICS_ALTERNATE_PURCHASE_REPORT` | Seller | JSON report |
| `GET_BRAND_ANALYTICS_ITEM_COMPARISON_REPORT` | Seller | JSON report |
| `GET_SALES_AND_TRAFFIC_REPORT` | Seller | JSON report |
| `GET_SALES_AND_TRAFFIC_REPORT_BY_DATE` | Seller | JSON report |
| `GET_SALES_AND_TRAFFIC_REPORT_BY_MONTH` | Seller | JSON report |
| `GET_ORDER_REPORT_DATA_SHIPPING` | Seller | XML report |
| `GET_XML_BROWSE_TREE_DATA` | Seller | XML report |
| `GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL` | Seller | XML report |
| `GET_SELLER_FEEDBACK_DATA` | Seller | Localized table report |
| `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL` | Seller | Tab-delimited report |
| `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL` | Seller | Tab-delimited report |
| `GET_FBA_STORAGE_FEE_CHARGES_DATA` | Seller | Tab-delimited report |
| `GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL` | Seller | Tab-delimited report |
| `GET_FLAT_FILE_OPEN_LISTINGS_DATA` | Seller | Tab-delimited report |
| `GET_FBA_FULFILLMENT_REMOVAL_ORDER_DETAIL_DATA` | Seller | Tab-delimited report |
| `GET_FLAT_FILE_ACTIONABLE_ORDER_DATA_SHIPPING` | Seller | Tab-delimited report |
| `GET_LEDGER_DETAIL_VIEW_DATA` | Seller | Tab-delimited report |
| `GET_LEDGER_SUMMARY_VIEW_DATA` | Seller | Tab-delimited report |
| `GET_FBA_REIMBURSEMENTS_DATA` | Seller | Tab-delimited report |
| `GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA` | Seller | Tab-delimited report |
| `GET_AFN_INVENTORY_DATA` | Seller | Tab-delimited report |
| `GET_AFN_INVENTORY_DATA_BY_COUNTRY` | Seller | Tab-delimited report |
| `GET_FBA_INVENTORY_PLANNING_DATA` | Seller | Tab-delimited report |
| `GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE` | Seller | Tab-delimited report |
| `GET_FLAT_FILE_ARCHIVED_ORDERS_DATA_BY_ORDER_DATE` | Seller | Tab-delimited report |
| `GET_MERCHANTS_LISTINGS_FYP_REPORT` | Seller | Tab-delimited report |
| `GET_MERCHANT_CANCELLED_LISTINGS_DATA` | Seller | Tab-delimited report |
| `GET_MERCHANT_LISTINGS_DATA_BACK_COMPAT` | Seller | Tab-delimited report |
| `GET_STRANDED_INVENTORY_UI_DATA` | Seller | Tab-delimited report |
| `GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA` | Seller | Tab-delimited report |
| `GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_PROMOTION_DATA` | Seller | Tab-delimited report |
| `GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA` | Seller | Tab-delimited report |
| `GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT` | Seller | Tab-delimited report |
| `GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_REPLACEMENT_DATA` | Seller | Tab-delimited report |
| `GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA` | Seller | Tab-delimited report |
| `GET_MERCHANT_LISTINGS_ALL_DATA` | Seller | Tab-delimited report |
| `GET_MERCHANT_LISTINGS_DATA` | Seller | Tab-delimited report |
| `GET_MERCHANT_LISTINGS_INACTIVE_DATA` | Seller | Tab-delimited report |
| `GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2` | Seller | Tab-delimited report |

`Orders` and `OrderItems` use Orders v0 and preserve its data and restricted-token semantics. Amazon lists March 27, 2027 as the removal date for the Orders v0 operations used here. [SP-API Deprecations](https://developer-docs.amazon/sp-api/docs/sp-api-deprecations)

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `resource` | String | Yes | — | Selects the canonical resource name. |
| `account_type` | String | No | `SELLER` | SELLER or VENDOR. Match the selected resource. |
| `environment` | String | No | `PRODUCTION` | PRODUCTION or SANDBOX. |
| `region` | String | No | `US` | Marketplace country code. |
| `client_id` | String | Yes | — | Login with Amazon OAuth client ID. |
| `client_secret` | String | Yes | — | Login with Amazon OAuth client secret. |
| `token` | String | Yes | — | Selling partner refresh token. |
| `include_pii` | Boolean | No | `false` | Enable scoped restricted data access for supported resources. |
| `start_time` | String | No | — | Inclusive beginning of the general replication window. |
| `end_time` | String | No | — | Exclusive general cutoff, capped by provider availability. |
| `period_in_days` | Integer | No | `90` | Maximum range size in days, from 1 to 365. Resources may apply lower caps. |
| `financial_events_step` | String | No | `180D` | Finance slice duration using positive integer hours or days, at most 180 days. |
| `financial_page_size` | Integer | No | `100` | Finance page size, from 1 to 100. |
| `inbound_replication_mode` | String | No | `ROLLING` | ROLLING or FIXED for FBA inbound. |
| `inbound_rolling_days` | Integer | No | `30` | FBA rolling lookback, from 1 to 730 days. |
| `inbound_start_datetime` | String | No | — | Inclusive FBA start, required for FIXED. |
| `inbound_end_datetime` | String | No | — | Exclusive FBA end for FIXED. Defaults to the run start time. |
| `report_options_list` | String | No | `[]` | JSON array string of per-view report options. |
| `asin_granularity` | String | No | `PARENT` | PARENT, CHILD or SKU for Sales and Traffic by ASIN. |
| `report_lookback_hours` | Integer | No | `0` | Extra hours added before an explicit report start, from 0 to 720. |
| `max_done_report_age_hours` | Integer | No | `0` | Maximum age in hours for matching completed reports, from 0 to 72. Zero disables completed report reuse. |
| `treat_cancelled_as_empty` | Boolean | No | `false` | Treat a report created during the current read as empty if CANCELLED without a document. |
| `max_requests` | Integer | No | `10000` | Maximum provider steps and report output batches for one split, from 1 to 100000. |
| `max_attempts` | Integer | No | `4` | Maximum HTTP attempts for retryable operations, from 1 to 10. |
| `max_rate_limit_retries` | Integer | No | `3` | Maximum HTTP 429 retries per operation, from 0 to 9. |
| `stop_sync_on_rate_limit` | Boolean | No | `false` | Fail on the first HTTP 429 during a report request. |
| `request_timeout_ms` | Integer | No | `30000` | One request budget including retries, from 100 to 300000 milliseconds. |
| `report_timeout_ms` | Integer | No | `3600000` | Time to await one report, from 1000 to 14400000 milliseconds. |
| `poll_interval_ms` | Integer | No | `30000` | Interval between report status checks, from 100 to 300000 milliseconds. |
| `max_response_bytes` | Integer | No | `8388608` | Maximum response or decoded document size, from 1024 to 67108864 bytes. |
| `max_report_rows` | Integer | No | `100000` | Maximum decoded rows per report, from 1 to 500000. |
| `sandbox_origin` | String | No | — | Custom SANDBOX HTTPS origin. Requires trust_certificate. |
| `trust_certificate` | String | No | — | Worker-local PEM trust anchor for a custom sandbox. |

The supported region codes are `AE`, `AU`, `BE`, `BR`, `CA`, `DE`, `EG`, `ES`, `FR`, `GB` (alias `UK`), `IE`, `IN`, `IT`, `JP`, `MX`, `NL`, `PL`, `SA`, `SE`, `SG`, `TR`, and `US`. The connector chooses the matching SP-API regional endpoint and marketplace ID. [SP-API Endpoints](https://developer-docs.amazon/sp-api/docs/sp-api-endpoints)

## Time windows and report options

- Supply whole-second RFC 3339 timestamps with an offset. The source converts them to UTC and fixes the run's end when it starts. Orders and Finances hold back the most recent two minutes. FBA inbound uses its own fixed or rolling interval. The default general lookback is up to 730 days, subject to the resource's available history.
- Day and month report views request only complete UTC calendar units within the selected interval. Vendor daily analytics use a 72-hour default publication delay. Amazon's vendor store metrics follow the provider's Pacific time reporting calendar. Brand Analytics views select the latest previous complete Sunday-to-Saturday week relative to the end time. Vendor Forecasting reads the current snapshot for the selected selling program. [Analytics Reports](https://developer-docs.amazon/sp-api/docs/report-type-values-analytics)
- `report_options_list` is a JSON string containing an array of objects. Each entry has a unique `stream_name` and an `options_list` array of unique `option_name` and string `option_value` pairs. The selected resource receives its own options. Fixed options define each view. Brand Analytics uses `reportPeriod=WEEK`, the Forecasting views select `FRESH` or `RETAIL`, and Sales and Traffic uses `dateGranularity=DAY` or `MONTH`.
- Vendor Sales, Inventory, Traffic, and Net Pure Product Margin views use `reportPeriod=DAY`. Vendor Sales and Inventory require a `distributorView` of `MANUFACTURING` or `SOURCING` and a supported `sellingProgram`. Sales and Inventory accept `RETAIL` or `FRESH`. Sales also accepts `BUSINESS`. Vendor real-time inventory retains the provider's hourly rows in each daily request. [Vendor Retail Analytics Reports](https://developer-docs.amazon/sp-api/docs/report-type-values-analytics#vendor-retail-analytics-reports)
- Both the scheduled `GET_ORDER_REPORT_DATA_SHIPPING` XML report and automatically generated settlement reports are read from reports already available to the account by creation time, within the latest 89 days. Configure the shipping schedule in Amazon before the job. The XML report requires `include_pii = true` and a report explicitly scoped to the selected marketplace. `GET_FLAT_FILE_ACTIONABLE_ORDER_DATA_SHIPPING` can be requested and also requires restricted access. [Order Reports](https://developer-docs.amazon/sp-api/docs/report-type-values-order)

## Authentication, delivery and recovery

Use an authorized Login with Amazon client and selling-partner refresh token. The connector uses SP-API's regional HTTPS origin with `x-amz-access-token`. `include_pii = true` requests a Restricted Data Token (RDT) for Orders, OrderItems, VendorDirectFulfillmentShipping, and restricted report document operations. Vendor shipping labels are available for up to 90 days and require `include_pii = true`. A role denial fails the read. Configure the corresponding restricted roles for the selling partner and authorize the destination to store this data. A custom sandbox requires its worker-local trust anchor and hostname verification. Report contents are downloaded from the provider's HTTPS S3 or CloudFront URL without API authorization headers. The source never follows document redirects. [Tokens API](https://developer-docs.amazon/sp-api/docs/tokens-api-use-case-guide) · [Retrieve a Report](https://developer-docs.amazon/sp-api/docs/retrieve-a-report)

External report reuse requires matching report type, marketplace, date bounds, and explicitly reported option values. Amazon's ordinary report metadata omits options, so the absence of those values leaves the match unproven. The source resumes an owned report ID from its checkpoint. `max_done_report_age_hours` also bounds reuse of completed reports. A newly requested report with an ambiguous create outcome fails with an actionable error if a matching report cannot be established. A previously saved creation intent is rechecked before any new submission. `FATAL`, unknown statuses, and unconfirmed `CANCELLED` results fail the job. An expired document URL can be refreshed through report metadata within the request budget.

Zeta checkpoints retain the current page, parent, time slice, report ID, phase and document row position. When resuming a partly emitted document, the source computes its digest again and fails if its content changed. A provider cursor can expire, external data can change, and an older checkpoint can replay records or recreate an earlier report. Use the record identity and business fields for downstream deduplication as required.

## Example

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
