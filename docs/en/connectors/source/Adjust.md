import ChangeLog from '../changelog/connector-http-adjust.md';

# Adjust

{/*
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
*/}

The `Adjust` source reads a bounded daily report from the Adjust Report Service JSON API. Install `connector-http-adjust` and provide an Adjust API token with access to the requested reports. [Adjust JSON reports](https://dev.adjust.com/en/api/rs-api/reports) · [Authentication](https://dev.adjust.com/en/api/rs-api/authentication)

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes | | Adjust API token sent in the Bearer header. |
| `start_date` | String | Yes | | First report day, inclusive, in `YYYY-MM-DD` format. |
| `dimensions` | `List<String>` | Yes | | Ordered dimension slugs, such as `app` and `country`. An empty list requests only the required `day` dimension. |
| `metrics` | `List<String>` | Yes | | Nonempty ordered list of metric slugs, such as `installs` or `network_cost`. |
| `additional_metrics` | `List<String>` | No | `[]` | Additional metric slugs, such as cohort or app-specific metrics. |
| `metric_scales` | `Map<String, String>` | No | `{}` | Overrides decimal scale by metric slug, from 0 to 38. Omitted metrics use scale 18. |
| `until_today` | Boolean | No | false | Include today. The default stops at yesterday. |
| `utc_offset` | String | No | `+00:00` | Fixed report offset in `+HH:MM` or `-HH:MM` format, within 14 hours of UTC. |
| `base_url` | String | No | `https://automate.adjust.com` | Trusted API origin. An optional trailing slash is allowed. |
| `allow_insecure_http` | Boolean | No | false | Allows cleartext HTTP for an explicitly trusted, isolated origin. |
| `request_timeout_ms` | Integer | No | 30000 | Total timeout per report day, including attempts and retry delays, from 1 to 300000 milliseconds. |
| `max_attempts` | Integer | No | 4 | Maximum HTTP attempts per report day, from 1 to 8. |
| common-options | | No | | [Source common options](../common-options/source-common-options.md). |

## Dynamic schema

The `adjust_report` table starts with a non-null `day DATE` column. It is followed by the selected dimensions as nullable `STRING` columns, then the metrics and additional metrics as nullable `DECIMAL(38, scale)` columns. If `day` is listed in `dimensions`, it still appears once. Column names must otherwise be unique. Use Adjust's dimension and metric slugs from the report API or the metadata endpoints. [Filters data](https://dev.adjust.com/en/api/rs-api/filters-data) · [Event slugs](https://dev.adjust.com/en/api/rs-api/events)

Numeric JSON values and numeric strings are converted exactly. An invalid or out-of-range decimal stops that day without rounding. A scale of 0 allows up to 38 integer digits and is useful for counts. Missing and null dimensions or metrics become null. The connector emits `rows` and omits totals and auxiliary fields.

## Dates, limits and recovery

The source sends one request per day with a fixed offset and ISO date formatting. The last day is captured when the scan starts. With `until_today = true`, the latest day can still change at the provider. UTC is used unless you supply an offset. Named time zones and daylight-saving transitions are outside the fixed-offset contract.

An empty `rows` array or HTTP 204 advances the scan without records. A response with nonempty pagination, warnings, a repeated dimension group, or a row from another day fails that day before publishing its records. The JSON report API does not describe a continuation request. The job supports up to 3660 days, 96 output columns and 20000 rows per day. Each response can contain at most 8 MiB before and after decompression.

Use source parallelism 1. The checkpoint retains the original end date and next unfinished report day. Restore requires the same report parameters and can use a rotated token. Checkpoints store neither the token nor report rows. A report can change between requests. Data emitted after the most recent completed checkpoint may be read again after recovery.

HTTPS uses default certificate trust and hostname validation. Redirects and authorization failures stop the request. HTTP 429, 500, 502, 503, and 504 are eligible for bounded retry. A server-provided `Retry-After` lower bound must fit the report day's total time budget. [Adjust rate limits](https://dev.adjust.com/en/api/rs-api/rate-limits)

## Example

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}

source {
  Adjust {
    token = ${?ADJUST_TOKEN}
    start_date = "2026-10-01"
    dimensions = ["app", "country"]
    metrics = ["installs", "network_cost"]
    additional_metrics = ["purchase_events"]
    metric_scales = { installs = "0", network_cost = "12", purchase_events = "0" }
    until_today = false
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
