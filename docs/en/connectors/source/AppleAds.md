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

import ChangeLog from '../changelog/connector-http-apple-search-ads.md';

# Apple Ads

Install `connector-http-apple-search-ads` and use the `AppleAds` source in a batch job. Obtain an Apple Ads API client ID and externally signed ES256 client-secret JWT for an API user who can read the selected App Store advertising account. [Apple Ads Platform access](https://developer.apple.com/documentation/apple-ads-platform-api/access-overview) · [OAuth token exchange](https://developer.apple.com/documentation/apple-ads-platform-api/implementing-oauth-for-the-apple-ads-platform-api)

## API modes

| `api_mode` | Account option | API origin |
| --- | --- | --- |
| `PLATFORM_V1` (default) | `ad_account_id` | `https://api.ads.apple.com` |
| `LEGACY_V5` | `org_id` | `https://api.searchads.apple.com` |

Apple announced that Campaign Management v5 will shut down on January 26, 2027. Use the Platform ad account ID when migrating. It is a different identifier from the legacy organization ID, and a checkpoint belongs to the original mode and account. The connector sends only documented read operations. [Campaign Management API sunset](https://developer.apple.com/documentation/apple_ads/reports)

## Resources

| Resource | Data |
| --- | --- |
| `campaigns` | App Store campaigns |
| `adgroups` | Ad groups across the discovered campaigns |
| `keywords` | Targeting keywords across campaigns and ad groups |
| `ads` | Ads across campaigns and ad groups |
| `campaigns_report_daily` | Daily campaign metrics |
| `adgroups_report_daily` | Daily ad group metrics |
| `keywords_report_daily` | Daily targeting keyword metrics |
| `ads_report_daily` | Daily ad metrics |

Reports request country or region grouping. A one-day Platform request uses the provider's total for that day. Geography is null if Apple supplies an aggregate without a geographic value. Reports with a date breakdown emit each daily observation with the reported date and region. Account-wide summaries are excluded. Parent discovery covers campaigns and ad groups independently of report windows. Provider status and permission errors fail the source.

## Options

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `api_mode` | string | `PLATFORM_V1` | API version and account namespace |
| `ad_account_id` | string | none | Positive Platform ad account ID, required in Platform mode |
| `org_id` | string | none | Positive legacy organization ID, required in legacy mode |
| `client_id` | string | required | Apple Ads API user client identifier |
| `client_secret` | string | required | Externally signed ES256 client-secret JWT |
| `resources` | list of strings | all eight | Resource names in read order |
| `start_date` | string | none | Inclusive `YYYY-MM-DD` start, required if a daily resource is selected |
| `end_date` | string | yesterday in the chosen calendar | Inclusive `YYYY-MM-DD` end |
| `report_cursor_date` | string | none | Previously processed report date supplied by the caller when starting a new batch |
| `lookback_days` | integer | 30 | Revisit this many days before a supplied report cursor, between 0 and 30 |
| `timezone` | string | `UTC` | Apple reports in `UTC` or `ORTZ` |
| `organization_timezone` | string | none | IANA timezone matching Apple's organization calendar, required for `ORTZ` |
| `report_window_days` | integer | 7 | Maximum inclusive days per report request, from 1 through 90 |
| `page_size` | integer | 1000 | Maximum provider entities per page, from 1 through 1000 |
| `max_pages_per_partition` | integer | 10000 | Maximum pages per parent collection or report window |
| `request_timeout_ms` | integer | 30000 | Deadline for one full HTTPS attempt |
| `operation_timeout_ms` | integer | 120000 | Deadline including token acquisition, pacing and retries |
| `min_request_interval_ms` | integer | 100 | Minimum interval between HTTPS attempts |
| `max_retries` | integer | 3 | Number of additional attempts for eligible responses or read failures |
| `max_response_bytes` | integer | 16777216 | Independent limits for transfer and decoded JSON bytes |
| `api_origin` | string | mode-specific origin | Optional HTTPS origin override for Apple Ads requests |
| `oauth_origin` | string | `https://appleid.apple.com` | HTTPS origin for Apple token exchange |

Keep daily readings within the last 90 days of the chosen calendar. The source uses the later of `start_date` and a supplied `report_cursor_date` minus `lookback_days`. A new batch with no cursor starts at `start_date`. Dates and the reporting calendar are frozen for checkpoint restore. The configured date range should contain closed dates only. [Platform date ranges](https://developer.apple.com/documentation/apple-ads-platform-api/timerange) · [Legacy daily windows](https://developer.apple.com/documentation/apple_ads/reportingrequest)

## Output and recovery

The source produces string columns `resource`, `api_mode`, `account_id`, `entity_id`, `campaign_id`, `ad_group_id`, `report_date`, `country_or_region` and `record`. Fields that do not apply to a resource are null. The `record` JSON contains Apple entity fields or an object with the report `metadata` and selected `metrics`. Monetary amounts retain their provider representation and precision.

One active reader resolves each parent and report page. A checkpoint saves the mode, account, frozen windows, active pagination stack and accepted daily row position. On restore, the source verifies that the saved page agrees with the offset and fingerprint before resuming. A changed page fails with a recovery error. Apple may update advertising or attribution data during a scan. End-to-end delivery depends on the selected sink.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  AppleAds {
    ad_account_id = "123456789"
    client_id = "<apple-api-user-client-id>"
    client_secret = ${APPLE_ADS_CLIENT_SECRET}
    resources = ["campaigns", "adgroups", "keywords", "ads"]
  }
}

sink {
  Console {}
}
```

Set `client_secret` through your job configuration's secret injection mechanism. The engine must trust the HTTPS certificates for both origins.

<ChangeLog />
