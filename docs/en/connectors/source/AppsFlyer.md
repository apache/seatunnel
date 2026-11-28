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

import ChangeLog from '../changelog/connector-http-appsflyer.md';

# AppsFlyer

Reads AppsFlyer v5 Pull API CSV reports in a bounded SeaTunnel Zeta batch with source parallelism 1.
Install `connector-http-appsflyer` using `bin/install-plugin.sh`. Create a V2 Pull API token with access
to the selected app and reports. See [raw reports](https://support.appsflyer.com/hc/en-us/articles/360007530258-Pull-API-raw-data)
and [aggregate reports](https://support.appsflyer.com/hc/en-us/articles/207034346-Pull-API-aggregate-data).

## Resources

| Resource | v5 report | Source cursor |
| --- | --- | --- |
| `in_app_events` | `in_app_events_report` | `event_time` |
| `organic_in_app_events` | `organic_in_app_events_report` | `event_time` |
| `retargeting_in_app_events` | `in-app-events-retarget` | `event_time` |
| `installs` | `installs_report` | `install_time` |
| `organic_installs` | `organic_installs_report` | `install_time` |
| `retargeting_installs` | `installs-retarget` | `install_time` |
| `uninstall_events` | `uninstall_events_report` | `event_time` |
| `organic_uninstall_events` | `organic_uninstall_events_report` | `event_time` |
| `daily_report` | `daily_report` | `date` |
| `retargeting_daily_report` | `daily_report` + `reattr=true` | `date` |
| `partners_report` | `partners_by_date_report` | `date` |
| `retargeting_partners_report` | `partners_by_date_report` + `reattr=true` | `date` |
| `partners_events_report` | `partners_by_date_report` | `date` |
| `retargeting_partners_events_report` | `partners_by_date_report` + `reattr=true` | `date` |
| `geo_report` | `geo_by_date_report` | `date` |
| `retargeting_geo_report` | `geo_by_date_report` + `reattr=true` | `date` |
| `geo_events_report` | `geo_by_date_report` | `date` |
| `retargeting_geo_events_report` | `geo_by_date_report` + `reattr=true` | `date` |

Partners and geo event views unpack event-specific metric columns into one row per event name per CSV row.
When both a summary and its event view are selected, they share the same download. Retargeting and
user acquisition use distinct requests.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `token` | string | required | V2 bearer token. Store it outside the job file. |
| `app_id` | string | required | AppsFlyer app ID. Provide the unencoded ID as a single path segment. |
| `start_date` | string | required | Inclusive start in the configured timezone. |
| `end_date` | string | optional | Exclusive end. When omitted, raw reports end at the initial job start rounded down to the second, and aggregate reports include the initial app-local date. |
| `timezone` | string | `UTC` | IANA timezone matching the AppsFlyer app setting. |
| `resources` | list of strings | All 18 | One or more distinct names from the resource table. |
| `api_url` | string | `https://hq1.appsflyer.com` | AppsFlyer HTTPS origin, with optional port. |
| `download_origins` | list of strings | `[]` | Up to eight distinct HTTPS origins allowed for delegated report downloads. Use the exact trusted host and port supplied for your environment. |
| `max_window_records` | int | `100000` | Split a window when incoming rows reach this threshold or expanded rows exceed it. Range 2–1000000. API row caps still apply. |
| `max_response_bytes` | int | `33554432` | Separate ceilings for encoded and decoded report bytes. Range 1024–134217728. |
| `max_buffer_bytes` | int | `33554432` | Maximum UTF-8 JSON payload bytes staged from one response. Range 1024–134217728. |
| `max_field_chars` | int | `262144` | Maximum UTF-16 units per CSV field. Range 16–1048576. |
| `max_requests` | int | `1000` | Total HTTP attempts across report windows, retries and redirects per reader execution. Range 1–100000. |
| `connect_timeout_ms` | int | `10000` | TCP connection timeout. Range 1–300000. |
| `read_timeout_ms` | int | `15000` | TLS handshake and response inactivity timeout. Range 1–300000. |
| `request_timeout_ms` | int | `120000` | Total budget for a window, including redirects, response reading and retries. Range 1–300000. |
| `max_attempts` | int | `3` | Initial request plus retry attempts per window. Range 1–10. |

## Dates, limits and recovery

Use `yyyy-MM-dd` when any aggregate view is selected. Raw-only jobs also accept second-precision
ISO timestamps with an offset or an unambiguous app-local timestamp such as `2026-11-01 03:00:00`.
Aggregate API requests convert the exclusive job end to an inclusive report date. Raw API requests
use an exclusive timestamp end and prefer the app's configured currency. Set the matching app
timezone so the provider interprets the requested dates accurately.

The connector enforces the standard 90-day raw and 25-month aggregate windows. Actual availability
and freshness depend on the account and report. A capped or oversized report is subdivided at
non-overlapping seconds for raw reports and whole days for aggregate reports. If the smallest
available interval still hits a limit, the job fails without emitting an incomplete version of
that interval. Retries and subdivisions consume the request budget and may consume provider quota.
Throttling retries honor the budget and `Retry-After`. Daily quota exhaustion fails immediately.
See [data availability](https://support.appsflyer.com/hc/en-us/articles/4415473374865-Data-availability-windows)
and [provider quotas](https://support.appsflyer.com/hc/en-us/articles/207034366-Report-generation-quotas-rate-limitations).

HTTPS uses the JVM trust store and normal hostname validation. A download may follow at most three
redirects, and a delegated origin must be listed in `download_origins`. The bearer token is only
sent to the API origin and stays detached for the rest of a redirect chain after crossing origins.
The returned signed download URL is used only in memory.

A Zeta checkpoint records pending time windows and the row position in an unfinished report. On
restore, completed windows are skipped. If rows from the unfinished report were emitted before the
checkpoint, its re-download must have the same records and row ordering. Its digest is compared
before re-emission. The initial end and any adaptive splits are carried in checkpoint state. Provider
reports can change or become unavailable. There is no provider snapshot token, so downstream
commit behavior depends on the sink and a separate fresh job can observe revised data.

## Output

Each row contains five strings: `resource`, `app_id`, `report_time`, `time_zone`, and `data`.
The `data` value is a JSON object serialized as a string. CSV labels are normalized into keys, and
labels that collide remain under `additional_columns` using their original names. Raw CSV text
including empty values, `N/A`, `NULL`, commas, and embedded line breaks is preserved. Known
raw booleans and timestamps and known aggregate numeric metrics use their JSON types or null for
a missing typed value. Raw monetary text remains exact and known aggregate metrics use exact
decimal numbers. Percentage values keep the reported units, for example `12.25%` becomes `12.25`.
Timestamps with offsets are normalized to UTC. Offset-free provider values remain local,
with `time_zone` identifying the requested timezone. Event views add `event_name`,
`event_unique_users`, `event_counter`, and `event_sales`. Metrics absent for an event are null.

## Example

Choose report dates within the current retention window and supply `APPSFLYER_TOKEN` through a
protected configuration variable. For delegated downloads, add your trusted `download_origins`.

```hocon
env {
    job.mode = "BATCH"
    parallelism = 1
}
source {
    AppsFlyer {
        token = ${APPSFLYER_TOKEN}
        app_id = "com.example.app"
        start_date = "2026-11-01"
        end_date = "2026-11-03"
        timezone = "America/New_York"
        resources = ["in_app_events", "organic_installs"]
    }
}
sink {
    Console {}
}
```

## Changelog

<ChangeLog />
