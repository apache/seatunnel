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

import ChangeLog from '../changelog/connector-http-amazon-ads.md';

# Amazon Ads

Install `connector-http-amazon-ads` and configure `AmazonAds` in a batch job. Use a Login with Amazon client ID, client secret and refresh token from an approved Amazon Ads integration. Amazon Ads authorization uses profile-scoped per-service APIs and Reporting v3. [Amazon Ads API collection](https://github.com/amzn/ads-advanced-tools-docs/tree/main/postman) · [Authorization](https://advertising.amazon.com/API/docs/en-us/guides/account-management/authorization/authorization-grants)

## Direct resources

| Resource | API operation |
| --- | --- |
| `profiles` | GET `/v2/profiles` |
| `portfolios` | POST `/portfolios/list` |
| `sponsored_display_campaigns` | GET `/sd/campaigns` |
| `sponsored_display_ad_groups` | GET `/sd/adGroups` |
| `sponsored_display_product_ads` | GET `/sd/productAds` |
| `sponsored_display_targetings` | GET `/sd/targets` |
| `sponsored_display_creatives` | GET `/sd/creatives` |
| `sponsored_display_budget_rules` | GET `/sp/budgetRules` |
| `sponsored_brands_keywords` | GET `/sb/keywords` |
| `sponsored_brands_campaigns` | POST `/sb/v4/campaigns/list` |
| `sponsored_brands_ad_groups` | POST `/sb/v4/adGroups/list` |
| `sponsored_brands_ads` | POST `/sb/v4/ads/list` |
| `sponsored_product_campaigns` | POST `/sp/campaigns/list` |
| `sponsored_product_ad_groups` | POST `/sp/adGroups/list` |
| `sponsored_product_keywords` | POST `/sp/keywords/list` |
| `sponsored_product_negative_keywords` | POST `/sp/negativeKeywords/list` |
| `sponsored_product_campaign_negative_keywords` | POST `/sp/campaignNegativeKeywords/list` |
| `sponsored_product_ads` | POST `/sp/productAds/list` |
| `sponsored_product_targetings` | POST `/sp/targets/list` |
| `sponsored_product_ad_group_bid_recommendations` | POST `/sp/targets/bid/recommendations` |
| `sponsored_product_ad_group_suggested_keywords` | POST `/sp/targets/keywords/recommendations` |
| `attribution_report_products` | POST `/attribution/report` |
| `attribution_report_performance_adgroup` | POST `/attribution/report` |
| `attribution_report_performance_campaign` | POST `/attribution/report` |
| `attribution_report_performance_creative` | POST `/attribution/report` |

The profile listing can export every accessible seller and vendor profile. The source applies the union of `profiles` and `marketplace_ids` to each child resource. Recommendation reads still discover Sponsored Products ad groups when ad-group output is deselected. The `sponsored_display_budget_rules` resource reads the Sponsored Products budget-rules endpoint. Select the resources your profile can access. Permission and unsupported-product responses can fail the job.

## Asynchronous reports

| Summary resource | Daily resource | Report type |
| --- | --- | --- |
| `sponsored_brands_v3_report_stream` | `sponsored_brands_v3_report_stream_daily` | `sbPurchasedProduct` |
| `sponsored_brands_campaigns_report_stream` | `sponsored_brands_campaigns_report_stream_daily` | `sbCampaigns` |
| `sponsored_brands_adgroups_report_stream` | `sponsored_brands_adgroups_report_stream_daily` | `sbAdGroup` |
| `sponsored_brands_ads_report_stream` | `sponsored_brands_ads_report_stream_daily` | `sbAds` |
| `sponsored_display_campaigns_report_stream` | `sponsored_display_campaigns_report_stream_daily` | `sdCampaigns` |
| `sponsored_display_adgroups_report_stream` | `sponsored_display_adgroups_report_stream_daily` | `sdAdGroup` |
| `sponsored_display_productads_report_stream` | `sponsored_display_productads_report_stream_daily` | `sdAdvertisedProduct` |
| `sponsored_display_targets_report_stream` | `sponsored_display_targets_report_stream_daily` | `sdTargeting` |
| `sponsored_display_asins_report_stream` | `sponsored_display_asins_report_stream_daily` | `sdPurchasedProduct` |
| `sponsored_products_campaigns_report_stream` | `sponsored_products_campaigns_report_stream_daily` | `spCampaigns` |
| `sponsored_products_adgroups_report_stream` | `sponsored_products_adgroups_report_stream_daily` | `spCampaigns` |
| `sponsored_products_keywords_report_stream` | `sponsored_products_keywords_report_stream_daily` | `spTargeting` |
| `sponsored_products_targets_report_stream` | `sponsored_products_targets_report_stream_daily` | `spTargeting` |
| `sponsored_products_search_terms_report_stream` | `sponsored_products_search_terms_report_stream_daily` | `spSearchTerm` |
| `sponsored_products_productads_report_stream` | `sponsored_products_productads_report_stream_daily` | `spAdvertisedProduct` |
| `sponsored_products_asins_keywords_report_stream` | `sponsored_products_asins_keywords_report_stream_daily` | `spPurchasedProduct` |
| `sponsored_products_asins_targets_report_stream` | `sponsored_products_asins_targets_report_stream_daily` | `spPurchasedProduct` |

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `client_id` | String | Required | Login with Amazon client identifier. |
| `client_secret` | String | Required | Login with Amazon client secret. |
| `refresh_token` | String | Required | Refresh token granted for the Amazon Ads API. |
| `region` | String | `NA` | Regional advertising service, NA, EU, or FE. |
| `base_url` | String | Derived | Optional HTTPS override of the selected regional Ads API origin. |
| `oauth_base_url` | String | Derived | Optional HTTPS origin for the Login with Amazon token service. |
| `download_hosts` | List of strings | `[]` | Additional exact HTTPS hosts allowed to serve signed downloads. |
| `resources` | List of strings | `[]` | Unique resource names to read, all 59 when absent. |
| `profiles` | List of strings | `[]` | Profile IDs selected for child data, combined with marketplace IDs. |
| `marketplace_ids` | List of strings | `[]` | Marketplace IDs selected for child data, combined with profile IDs. |
| `state_filter` | List of strings | `[]` | Optional ENABLED, PAUSED or ARCHIVED states for Sponsored Product and Brand campaigns. |
| `start_date` | String | Derived | Optional inclusive lower report date in YYYY-MM-DD. |
| `end_date` | String | Derived | Optional inclusive upper report date in YYYY-MM-DD. |
| `look_back_window` | Integer | `3` | Additional overlapping days on a default rolling reporting scan, from 0 to 30. |
| `report_window_days` | Integer | `30` | Maximum days per asynchronous report, from 1 to 30. |
| `page_size` | Integer | `100` | Page size for supported read endpoints, from 1 to 100. |
| `max_pages` | Integer | `1000` | Maximum pages per profile and resource, from 1 to 10000. |
| `max_profiles` | Integer | `1000` | Maximum profiles loaded from the array endpoint, from 1 to 10000. |
| `max_attempts` | Integer | `4` | Maximum attempts per eligible API operation, from 1 to 8. |
| `request_interval_ms` | Integer | `100` | Minimum request spacing, from 10 to 60000 milliseconds. |
| `io_timeout_ms` | Integer | `15000` | Connection and read idle timeout, from 100 to 120000 milliseconds. |
| `request_deadline_ms` | Integer | `60000` | Total operation budget for API calls and retries, from 500 to 900000 milliseconds. |
| `poll_interval_ms` | Integer | `10000` | Interval for checking a pending report, from 100 to 60000 milliseconds. |
| `report_timeout_ms` | Integer | `10800000` | Report processing timeout, from 1000 to 14400000 milliseconds. |
| `duplicate_wait_ms` | Integer | `60000` | Wait before retrying a non-reusable duplicate, from 100 to 120000 milliseconds. |
| `download_deadline_ms` | Integer | `300000` | Maximum duration per report download including decoding, from 500 to 900000 milliseconds. |
| `max_response_bytes` | Integer | `8388608` | Maximum decoded API response size, from 1024 to 33554432 bytes. |
| `max_download_bytes` | Integer | `67108864` | Maximum compressed report bytes, from 1024 to 268435456. |
| `max_report_bytes` | Integer | `268435456` | Maximum decoded report bytes, from 1024 to 1073741824. |
| `max_report_rows` | Integer | `1000000` | Maximum rows in a report download, from 1 to 1000000. |

## Dates, downloads, and recovery

The regional Ads hosts are `advertising-api.amazon.com` (NA), `advertising-api-eu.amazon.com` (EU) and `advertising-api-fe.amazon.com` (FE). Login with Amazon defaults to `api.amazon.com/auth/o2/token`. Report downloads use separate verified HTTPS requests to Amazon S3 or explicitly configured hosts. They carry the signed path and no OAuth or advertising headers. Redirects fail before forwarding any request. [Login with Amazon token exchange](https://developer.amazon.com/docs/login-with-amazon/authorization-code-grant.html) · [Reporting v3](https://d1y2lf8k3vrkfu.cloudfront.net/openapi/en-us/dest/OfflineReport_prod_3p.json)

Reporting v3 windows use inclusive UTC dates. With no configured bounds, the source freezes the current UTC day and starts yesterday plus the configured number of days of lookback. The source clamps start dates to a 60-day history. Attribution uses the selected profile’s timezone, the same 60-day upper history bound, and defaults to that profile’s frozen day. Account- or report-specific rules can impose narrower limits. The job fails if the requested interval falls entirely outside its supported range.

Output fields are `resource`, `id`, `profile_id`, nullable `parent_id`, nullable `window_start`, nullable `window_end` and `record`, all strings. IDs use provider keys or a deterministic row identity. For asynchronous reports, the ID binds the resource, profile, request window and row position. `record` preserves the provider JSON. Report records add `profileId` and `reportDate`, the inclusive end date. Daily records retain their own `date`. Recommendations add the parent ad-group and campaign IDs.

A 425 can identify a previously requested report. The connector checks its authorized profile, request dates, complete configuration and status before reuse. If a create response is lost, a provider report may exist. Subsequent attempts can discover it through 425. The source checks and decodes report files into a private temporary spool within separate byte limits before emitting rows. It cleans up the file after use or cancellation.

Checkpoints retain the frozen interval, profile and resource positions, report ID, request fingerprint, and a digest of any partly emitted page or file. Restoring the same extraction parameters allows reauthorization and downloads the report through a newly fetched URL. Each fresh job begins at its configured dates. Collections can change between requests.

## Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  AmazonAds {
    client_id = "${amazon_ads_client_id}"
    client_secret = "${amazon_ads_client_secret}"
    refresh_token = "${amazon_ads_refresh_token}"
    region = "NA"
    resources = ["profiles", "sponsored_product_campaigns", "sponsored_products_campaigns_report_stream_daily"]
  }
}

sink {
  Console {}
}
```

<ChangeLog />
