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

import ChangeLog from "../changelog/connector-http-microsoft-advertising.md";

# Microsoft Advertising

> Microsoft Advertising v13 Source 连接器

## 描述

以有界批作业读取广告账户、营销活动、广告组、广告、报表及批量实体。注册 Microsoft Entra 应用，并取得已授权 `https://ads.microsoft.com/msads.manage offline_access` 的刷新令牌。配置开发者令牌，并确保授权用户可访问目标广告账户。上游仅创建报表及批量下载任务。

## 特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [检查点](../../architecture/fault-tolerance/checkpoint-mechanism.md)

一个 Source Reader 实际发起请求。先发现账户，再逐个遍历广告父子实体和异步资源。每次 SearchAccounts 使用一个供应商原生谓词。未配置名称时按授权用户 ID 查询，多个名称分别在同一授权下搜索，重复账户按 ID 合并。

## 资源范围

`resources` 为空时选择全部 77 个固定资源及已配置的自定义报表。选择子资源时，也会读取作用域所需的父资源。

- 广告实体 `accounts`、`campaigns`、`ad_groups` 及 `ads`
- 批量实体 `labels`、`ad_group_labels`、`app_install_ad_labels`、`app_install_ads`、`keywords`、`keyword_labels`、`campaign_labels` 及 `budget`
- 预算报表 `budget_summary_report`
- 自定义报表使用配置定义的名称。

下列每个报表系列都有 `_hourly`、`_daily`、`_weekly` 及 `_monthly` 变体。

- `age_gender_audience_report`
- `campaign_performance_report`
- `ad_group_performance_report`
- `keyword_performance_report`
- `geographic_performance_report`
- `ad_group_impression_performance_report`
- `campaign_impression_performance_report`
- `product_dimension_performance_report`
- `audience_performance_report`
- `goals_and_funnels_report`
- `account_impression_performance_report`
- `user_location_performance_report`
- `account_performance_report`
- `ad_performance_report`
- `search_query_performance_report`
- `product_search_query_performance_report`

报表日期包含起止两端。默认结束日以伦敦日历冻结，请求使用微软的伦敦报表时区。下载数据的小时桶为 UTC，`2026-07-15|7` 输出为 `2026-07-15T07:00:00+00:00`。微软将周、月及年汇总扩展为整周期，返回周期起始日可能早于请求下界。连接器按各报表及聚合的官方保留期调整下界，日期范围完全超出保留期时明确失败。当天数据可能仍在修订。[微软报表保留期](https://learn.microsoft.com/en-us/advertising/guides/report-data-retention-time-periods?view=bingads-13)。

自定义报表需要 `name`、`reporting_object`、`report_aggregation` 及 `report_columns`。可用请求类型如 `AccountPerformanceReportRequest`，选列应属于该类型。需要时间列时会追加缺失的 `TimePeriod`。`Summary` 不接受此列。`DayOfWeek` 和 `HourOfDay` 使用 `predefined_time`，支持 `Today`、`Yesterday`、`LastSevenDays`（默认）、`ThisMonth`、`LastMonth`、`LastThreeMonths` 及 `LastSixMonths`。输出包括对应的时间桶，以及 `StartOfTimePeriod`、`EndOfTimePeriod` 和 `TimePeriod` 日期。作业启动后，若伦敦日历已换日，尚未提交的预设日期报表会明确失败。名称默认转为小写下划线形式，设置 `disable_custom_report_names_camel_to_snake_conversion = true` 可保留原名。[微软报表列与聚合契约](https://learn.microsoft.com/en-us/advertising/guides/reports?view=bingads-13)。

### 支持的自定义报表请求类型

从下列请求类型中选用。聚合和列组合受相应的微软接口约束。

| 请求类型 | 请求类型 |
| --- | --- |
| `AccountPerformanceReportRequest` | `AdDynamicTextPerformanceReportRequest` |
| `AdExtensionByAdReportRequest` | `AdExtensionByKeywordReportRequest` |
| `AdExtensionDetailReportRequest` | `AdGroupPerformanceReportRequest` |
| `AdPerformanceReportRequest` | `AgeGenderAudienceReportRequest` |
| `AudiencePerformanceReportRequest` | `CallDetailReportRequest` |
| `CampaignPerformanceReportRequest` | `ConversionPerformanceReportRequest` |
| `DSAAutoTargetPerformanceReportRequest` | `DSACategoryPerformanceReportRequest` |
| `DSASearchQueryPerformanceReportRequest` | `DestinationUrlPerformanceReportRequest` |
| `GeographicPerformanceReportRequest` | `GoalsAndFunnelsReportRequest` |
| `HotelDimensionPerformanceReportRequest` | `HotelGroupPerformanceReportRequest` |
| `KeywordPerformanceReportRequest` | `NegativeKeywordConflictReportRequest` |
| `ProductDimensionPerformanceReportRequest` | `ProductMatchCountReportRequest` |
| `ProductNegativeKeywordConflictReportRequest` | `ProductPartitionPerformanceReportRequest` |
| `ProductPartitionUnitPerformanceReportRequest` | `ProductSearchQueryPerformanceReportRequest` |
| `ProfessionalDemographicsAudienceReportRequest` | `PublisherUsagePerformanceReportRequest` |
| `SearchCampaignChangeHistoryReportRequest` | `SearchQueryPerformanceReportRequest` |
| `ShareOfVoiceReportRequest` | `UserLocationPerformanceReportRequest` |

## 配置项

| 名称 | 类型 | 必须 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| developer_token | string | 是 |  | 开发者令牌。 |
| client_id | string | 是 |  | Entra 应用 ID。 |
| refresh_token | string | 是 |  | 已授权的 OAuth 刷新令牌。 |
| client_secret | string | 否 | 空 | 机密客户端密钥。公共客户端可省略。 |
| tenant_id | string | 否 | common | Entra 租户 ID 或别名。 |
| resources | list | 否 | [] | 要输出的资源。 |
| account_names | list | 否 | [] | 名称谓词对象，含 `operator`（`Equals` 或 `Contains`）及 `name`。 |
| custom_reports | list | 否 | [] | 自定义报表定义。 |
| reports_start_date | string | 否 | 上一年 1 月 1 日 | 最早报表日期（YYYY-MM-DD）。显式设置且未指定 `bulk_sync_from` 时也作为批量增量起点。 |
| reports_end_date | string | 否 | 伦敦日历当天 | 报表结束日（YYYY-MM-DD）。 |
| reports_resume_date | string | 否 |  | 新批作业可配置的上次报告游标。 |
| bulk_sync_from | string | 否 |  | RFC3339 批量增量起点。超出 30 天限制（留五分钟余量）时读取全量。 |
| tls_trust_certificate | string | 否 | JVM 默认 | 仅用于本 Source 的 PEM 或 base64 DER 信任锚，继续校验主机名。 |
| lookback_window | int | 否 | 0 | 在 `reports_resume_date` 前重读的天数，最大 90，并遵守最早报表日期。 |
| connect_timeout_ms | int | 否 | 10000 | TCP 连接超时。 |
| request_timeout_ms | int | 否 | 30000 | JSON 操作期限，含认证与重试。 |
| download_timeout_ms | int | 否 | 300000 | 下载、解档及首轮 CSV 校验期限。 |
| poll_interval_ms | int | 否 | 5000 | 异步等待期间的轮询间隔。 |
| poll_timeout_ms | int | 否 | 3600000 | 等待上游异步任务就绪的最长时间。 |
| max_retries | int | 否 | 3 | 允许重试的请求上限。 |
| max_retry_delay_ms | int | 否 | 30000 | 可接受的重试延迟上限，含 Retry-After。 |
| max_response_bytes | int | 否 | 8388608 | JSON 解压前后的字节上限。 |
| max_compressed_bytes | int | 否 | 134217728 | 下载压缩数据的字节上限。 |
| max_expanded_bytes | int | 否 | 536870912 | 解压后 CSV 的字节上限。 |
| max_csv_row_chars | int | 否 | 262144 | CSV 逻辑记录的字符上限。 |
| max_csv_columns | int | 否 | 512 | CSV 列数上限。 |
| max_csv_rows | int | 否 | 10000000 | 每份 CSV 的最大记录数，含元数据。 |
| max_account_pages | int | 否 | 10000 | 所有名称谓词合计的分页上限。 |
| max_accounts | int | 否 | 10000 | 去重账户数上限。 |

## 输出和恢复

- `resource`、`account_id` 及 `customer_id` 是字符串标识。
- `record_key` 是以字符串表示的有序 JSON 数组。批量标签关系以标签 ID 及父实体 ID 为复合身份。已声明的报表键用空字符串或零标识哨兵表达缺失维度。自定义报表及未声明稳定键的固定报表输出 null。
- `record` 为供应商记录的 JSON 字符串。REST 对象保留原类型及精度并补充缺失的父级标识。报表保留 CSV 金额、百分比、空格及 ID 的原字符串，空单元格或 `--` 转为 JSON null。批量 CSV 保留空字符串，并将可识别的 `Modified Time` 转为 RFC3339。

检查点保留授权用户、冻结日期、行位置及已知上游任务 ID。恢复时校验授权用户，并轮询同一任务取得新的签名 URL。分页或文件内容变更会在输出剩余记录前失败。签名下载不携带应用凭据。报表任务 ID 最长有效一天，批量任务 ID 最长有效两天。提交结果未知时明确失败，不自动重复申请。供应商明确要求全量同步时，可由增量请求改为一次全量请求。OAuth 轮换只保留在 Reader 内存中。恢复或启动新作业需配置微软仍可接受的刷新令牌。

一个实际 Reader 同时持有一份有界 CSV 及其压缩档案。微软配额可能与其它调用方共享。账户及广告实体可能在请求之间改变。独立批作业之间需显式设置下一次报表日期或批量增量起点。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  MicrosoftAdvertising {
    tenant_id = "common"
    client_id = "your-application-id"
    developer_token = ${MICROSOFT_ADVERTISING_DEVELOPER_TOKEN}
    refresh_token = ${MICROSOFT_ADVERTISING_REFRESH_TOKEN}
    account_names = [{ operator = "Equals", name = "Example Advertising Account" }]
    resources = ["accounts", "campaigns", "campaign_performance_report_daily", "daily_clicks"]
    custom_reports = [{
      name = "dailyClicks"
      reporting_object = "AccountPerformanceReportRequest"
      report_aggregation = "Daily"
      report_columns = ["AccountId", "Clicks"]
    }]
  }
}
```

## 变更日志

<ChangeLog />
