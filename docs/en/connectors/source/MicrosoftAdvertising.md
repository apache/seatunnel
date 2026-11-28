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

> Microsoft Advertising v13 source connector

## Description

Run a bounded batch scan of advertiser accounts, campaigns, ad groups, ads, Reporting jobs and Bulk downloads. Register a Microsoft Entra application and obtain a refresh token authorized for `https://ads.microsoft.com/msads.manage offline_access`. Supply a developer token and use an OAuth user with access to the required advertiser accounts. The only jobs created upstream are reporting and bulk download requests.

## Features

- [x] [Batch](../../introduction/concepts/connector-v2-features.md)
- [x] [Checkpoint](../../architecture/fault-tolerance/checkpoint-mechanism.md)

Use one active source reader. Account discovery precedes campaign hierarchy traversal and asynchronous jobs. The source uses one predicate per SearchAccounts call. With no name filters, it searches by the authenticated user ID. Each configured name is a separate provider search under that user, and repeated accounts are emitted once.

## Resources

An empty `resources` list includes 77 fixed resources and all configured custom reports. Selecting children also reads their parents for scope.

- Direct entities `accounts`, `campaigns`, `ad_groups` and `ads`
- Bulk entities `labels`, `ad_group_labels`, `app_install_ad_labels`, `app_install_ads`, `keywords`, `keyword_labels`, `campaign_labels` and `budget`
- Budget report `budget_summary_report`
- Custom reports use their configured resource names.

Each report family below provides `_hourly`, `_daily`, `_weekly` and `_monthly` variants.

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

Reporting dates are inclusive. The default end is the current London date and the request sends Microsoft's London ReportTimeZone. Downloaded hourly buckets are UTC, so `2026-07-15|7` becomes `2026-07-15T07:00:00+00:00`. Microsoft expands weekly, monthly and yearly requests to whole periods. A returned period can start before the requested first date. The documented family and aggregation retention advances the lower date and rejects a wholly expired range. Today's data can be provisional. [Reporting data retention](https://learn.microsoft.com/en-us/advertising/guides/report-data-retention-time-periods?view=bingads-13).

A custom report requires `name`, `reporting_object`, `report_aggregation` and `report_columns`. Use a concrete supported request such as `AccountPerformanceReportRequest` and columns from that request type. The source adds `TimePeriod` when the chosen aggregation uses that column and it was omitted. `Summary` excludes this column. `DayOfWeek` and `HourOfDay` use `predefined_time`. Choose `Today`, `Yesterday`, `LastSevenDays` (default), `ThisMonth`, `LastMonth`, `LastThreeMonths` or `LastSixMonths`. Their output includes the bucket and resolved `StartOfTimePeriod`, `EndOfTimePeriod` and `TimePeriod` dates. An undispatched predefined request fails if the London date has advanced since the scan began. Custom names are normalized to lowercase snake case by default. Set `disable_custom_report_names_camel_to_snake_conversion = true` to preserve a name. [Report request and column rules](https://learn.microsoft.com/en-us/advertising/guides/reports?view=bingads-13).

### Supported custom report objects

Choose a request object from this list. Microsoft specifies the valid aggregations and column combinations for each object.

| Request object | Request object |
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

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| developer_token | string | yes |  | Developer token. |
| client_id | string | yes |  | Entra application ID. |
| refresh_token | string | yes |  | Authorized OAuth refresh token. |
| client_secret | string | no | empty | Confidential client secret. Omit for a public application. |
| tenant_id | string | no | common | Entra tenant ID or alias. |
| resources | list | no | [] | Resources to emit. |
| account_names | list | no | [] | Optional objects with `operator` (`Equals` or `Contains`) and `name`. |
| custom_reports | list | no | [] | Custom report definitions. |
| reports_start_date | string | no | January 1 of the previous year | Earliest report date (YYYY-MM-DD). An explicit date also seeds a bulk delta if `bulk_sync_from` is omitted. |
| reports_end_date | string | no | today in London | Last reporting date (YYYY-MM-DD). |
| reports_resume_date | string | no |  | Optional prior reporting cursor for a new batch. |
| bulk_sync_from | string | no |  | Initial bulk delta boundary in RFC3339. Outside 30 days with a five-minute margin, request a full file. |
| tls_trust_certificate | string | no | JVM defaults | PEM or base64 DER trust anchor local to this source. Hostname validation remains enabled. |
| lookback_window | int | no | 0 | Days to replay before reports_resume_date without passing reports_start_date. |
| connect_timeout_ms | int | no | 10000 | TCP connection timeout. |
| request_timeout_ms | int | no | 30000 | Deadline for one JSON exchange including read retries. |
| download_timeout_ms | int | no | 300000 | Whole-file download and decoding deadline including retries. |
| poll_interval_ms | int | no | 5000 | Delay between asynchronous job polls. |
| poll_timeout_ms | int | no | 3600000 | Time allowed for the return of a completed asynchronous job. |
| max_retries | int | no | 3 | Retries after eligible transport failures and rate-limit rejections. |
| max_retry_delay_ms | int | no | 30000 | Maximum permitted Retry-After or backoff. |
| max_response_bytes | int | no | 8388608 | Maximum JSON response bytes on the wire and after decompression. |
| max_compressed_bytes | int | no | 134217728 | Maximum compressed download bytes. |
| max_expanded_bytes | int | no | 536870912 | Maximum expanded CSV bytes. |
| max_csv_row_chars | int | no | 262144 | Maximum characters in one logical CSV record. |
| max_csv_columns | int | no | 512 | Maximum number of CSV columns. |
| max_csv_rows | int | no | 10000000 | Maximum records per download including metadata. |
| max_account_pages | int | no | 10000 | Maximum SearchAccounts pages across predicates. |
| max_accounts | int | no | 10000 | Maximum distinct accounts retained for the scan. |

## Output and recovery

- `resource`, `account_id` and `customer_id` are string identifiers.
- `record_key` is an ordered JSON array encoded as a string where an identity is defined. Bulk label relations use the label ID and parent ID. Fixed report keys fill missing dimensions with an empty string or the zero identifier sentinel. Custom reports and fixed reports without a declared identity return null.
- `record` is the provider object encoded as JSON. REST types and precision are preserved, with missing parent identifiers added. Report CSV values remain strings, preserving percentages, amounts, spaces and identifiers. Empty and `--` report fields become JSON null. Bulk CSV retains empty strings and formats recognizable `Modified Time` fields as RFC3339.

Checkpoint state keeps the authorized user, frozen dates, record position and any confirmed job ID. Restore verifies the user and resumes the same known job using a newly acquired signed URL. A changed page or file fails before the remaining rows are emitted. Signed downloads carry no API credentials. Report IDs expire after at most one day and Bulk IDs after at most two days. An unknown submit outcome fails without automatically creating a second job. An explicit Bulk rejection requiring full sync can start one full download. OAuth refresh token rotation remains in reader memory. A new job or restore requires a configured refresh token Entra still accepts.

One active reader holds at most one validated CSV and its bounded compressed archive. Microsoft quotas are shared with other clients. Account and campaign collections can change between requests. Specify the next date or bulk delta explicitly between independent batch jobs.

## Example

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

## Changelog

<ChangeLog />
