import ChangeLog from '../changelog/connector-tiktok-ads.md';

# TikTokAds

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

## Description

Read one advertiser's TikTok API for Business synchronous integrated report through
`GET /open_api/v1.3/report/integrated/get/`. This source supports only `BASIC` reports,
`AUCTION` service, `AUCTION_AD` data level and regular pagination. It does not export raw
users/events, GMV Max reports, asynchronous reports, multiple advertisers or arbitrary filters.

## Key Features

- [x] Batch
- [ ] Stream
- [ ] Exactly-Once

Use source parallelism 1. No checkpoint page-offset recovery is provided: recovery replays
the report from page 1. The remote report is **not an immutable snapshot**. Total-count and
repeated-dimension checks detect some pagination drift but cannot guarantee snapshot consistency
or completeness. Metrics or rows can change between requests without those checks detecting it.
A failed job may already have emitted records to a nontransactional sink; replay may duplicate
or change those records. Choose downstream staging/reconciliation appropriate for reporting data.

## Source Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| token | string | yes | - | Authorized Access-Token for the advertiser, sent only as an HTTP header. Native key `token` participates in SeaTunnel log masking. |
| advertiser_id | string | yes | - | One numeric advertiser ID, not a Business Center ID. |
| data_level | string | yes | - | Must be `AUCTION_AD`. |
| start_date | string | yes | - | Inclusive `yyyy-MM-dd` in the advertiser's account timezone. |
| end_date | string | yes | - | Inclusive `yyyy-MM-dd`. At most 30 inclusive days with `stat_time_day`, 365 otherwise. |
| dimensions | list | yes | - | Exactly `["ad_id"]` or `["ad_id", "stat_time_day"]`. |
| metrics | list | yes | - | Nonempty unique subset of `spend`, `impressions`, `clicks`. |
| schema | config | yes | - | Exact requested dimensions and metrics, in any output order; types below. |
| page_size | int | no | 1000 | Requested rows per page, 1-1000. |
| max_report_rows | int | no | 100000 | Fail above this total row count, never truncate; 1-1000000. This does not bypass TikTok's ad-ID limit. |
| max_response_bytes | int | no | 4194304 | Response byte limit per page, 1024-16777216. |
| request_timeout_ms | int | no | 30000 | HTTP deadline including body, 100-120000 ms. |
| report_timeout_ms | int | no | 600000 | Report deadline, 100-3600000 ms, checked between records/requests and during retry waits. |
| max_retries | int | no | 3 | Additional attempts per page, 0-5, only for HTTP 429, 500, 502, 503, 504. |
| max_retry_wait_ms | int | no | 30000 | Maximum retry delay, 1000-120000 ms. Retry-After beyond this bound fails instead of retrying early. |
| mock_url | string | no | - | Test-only HTTP origin. Requires literal `token = "mock-token"`. Never use real credentials. Not an official emulator. |
| common-options | | no | - | Supports `parallelism = 1` and `plugin_output`. |

### Schema and Report Semantics

- `ad_id`: STRING. The API identifies Manual/Smart+ ads but **creatives** for Upgraded Smart+
  Ads through this dimension. This connector does not support `ad_id_v2`.
- `stat_time_day`: STRING. Preserve the API's `yyyy-MM-dd 00:00:00` advertiser-local day
  label, not an invented UTC instant. Values must fall inside the requested interval.
- `spend`: DECIMAL(p,s), precision at most 38. Currency units follow the advertiser account;
  no cents conversion or currency conversion is performed. Values that require rounding or
  exceed precision fail. Use a scale suitable for your report.
- `impressions`, `clicks`: BIGINT. Invalid, fractional or overflowing count values fail.
- Requested fields must be present as nonempty strings. Null, missing, malformed and
  unrepresentable values fail; none is silently changed into zero. No optional output fields
  are supplied by this first slice. Empty API results produce zero rows.

The API's default `ad_status = STATUS_NOT_DELETE` filter applies: **deleted ads are excluded**.
This is not an all-status historical export. There is no arbitrary filtering option or automatic
ad-ID partitioning. Requests explicitly use `query_lifetime=false` and `query_mode=REGULAR`.
Ordering is the API default, not a stable snapshot order.
Branded Mission data requires advertiser-level reporting and is not returned through this
connector's `ad_id` dimension.

### Errors and Limits

HTTP 200 with a nonzero API `code` fails without retry. Malformed envelopes, missing page
metadata, inconsistent page/page-size/total counts, incomplete pages, repeated dimension keys,
and changing totals fail. No retry is made for permission errors, parser errors, response
limits or transport errors. HTTP retries use bounded exponential waits and honor valid
Retry-After seconds or HTTP dates. Invalid Retry-After fails without echoing its value.

TikTok documents `X-Tt-Ads-Throttle` as an over-limit warning. The current synchronous report
reference describes a 20,000-ad-ID cap; older v1.3 change notes describe 10,000. This connector
**conservatively rejects every nonempty value** of that header, including unrecognized warnings.
It never logs the warning contents. Absence of the header does not prove complete data.
Pagination does not bypass the synchronous endpoint's truncation. This slice cannot export an
advertiser requiring filtered or asynchronous reporting. Synchronous API results can differ
from Ads Manager downloads.

Redirects, automatic HTTP retries, cookies and response compression are disabled. The production
origin is fixed to `https://business-api.tiktok.com`; there is no configurable authenticated
origin. Closing a reader aborts the current request and wakes retry waits. JVM/platform DNS
resolution and a blocked downstream collect can outlive the configured deadlines; these are
not hard wall-clock cancellation guarantees during those operations.

## Prerequisites and Validation

Obtain an authorized token with reporting access to the advertiser through TikTok API for
Business. Provide it through a protected configuration/environment mechanism available to the
job. Do not put the token in query parameters, schema field names, custom keys, or debug logs.
The mock endpoint accepts only a literal nonsecret sentinel to prevent accidentally forwarding
a real token.

Local mock tests and real-engine mock jobs verify connector protocol and engine integration,
not live advertiser permissions, production quota behavior, data freshness or completeness.
Live advertiser validation requires separately supplied authorized access.

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  TikTokAds {
    token = ${TIKTOK_ADS_TOKEN}
    advertiser_id = "123456789"
    data_level = "AUCTION_AD"
    start_date = "2026-09-01"
    end_date = "2026-09-02"
    dimensions = ["ad_id", "stat_time_day"]
    metrics = ["spend", "impressions", "clicks"]
    schema {
      fields {
        ad_id = string
        stat_time_day = string
        spend = "decimal(38, 6)"
        impressions = bigint
        clicks = bigint
      }
    }
  }
}
sink {
  Console {}
}
```

## References

- [Synchronous report API](https://business-api.tiktok.com/portal/docs/run-a-synchronous-report/v1.3)
- [Official SDK reporting contract](https://github.com/tiktok/tiktok-business-api-sdk/blob/main/python_sdk/docs/ReportingApi.md)
- [Basic report dimensions](https://business-api.tiktok.com/portal/docs?id=1751443956638721)

## Changelog

<ChangeLog />
