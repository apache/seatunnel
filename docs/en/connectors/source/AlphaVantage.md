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

import ChangeLog from '../changelog/connector-http-alpha-vantage.md';

# AlphaVantage

AlphaVantage reads an equity symbol’s price history and current quote from the [Alpha Vantage API](https://www.alphavantage.co/documentation/). Run it in batch mode with source parallelism 1. Each selected resource is a bounded query with no pagination or temporal cursor.

## Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## Resources

| Resource | API function | Resource-specific options |
| --- | --- | --- |
| `time_series_intraday` | `TIME_SERIES_INTRADAY` | `interval`, `adjusted`, `outputsize` |
| `time_series_daily` | `TIME_SERIES_DAILY` | `outputsize` |
| `time_series_daily_adjusted` | `TIME_SERIES_DAILY_ADJUSTED` | `outputsize` |
| `time_series_weekly` | `TIME_SERIES_WEEKLY` | — |
| `time_series_weekly_adjusted` | `TIME_SERIES_WEEKLY_ADJUSTED` | — |
| `time_series_monthly` | `TIME_SERIES_MONTHLY` | — |
| `time_series_monthly_adjusted` | `TIME_SERIES_MONTHLY_ADJUSTED` | — |
| `quote` | `GLOBAL_QUOTE` | — |

All eight resources are selected by default. Select resources explicitly according to the endpoints and request quota available to your API key. The intraday, daily and adjusted daily compact queries return up to 100 observations. Daily full requests the provider’s available history, and intraday full requests its recent 30-day window. Alpha Vantage controls endpoint, history and freshness entitlements. The connector reads weekly, monthly and quote responses as returned by those endpoints.

## Source options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes | — | Alpha Vantage API key. |
| `symbol` | String | Yes | — | One equity symbol, including an exchange suffix when required. |
| `resources` | String list | No | All eight in the order above | Ordered, unique selection from the resource table. |
| `api_url` | String | No | `https://www.alphavantage.co` | HTTPS origin with host `www.alphavantage.co` and optional port. |
| `interval` | String | No | `1min` | Intraday interval. One of `1min`, `5min`, `15min`, `30min`, or `60min`. |
| `adjusted` | Boolean | No | `false` | Request split and dividend adjustment for the intraday series. |
| `outputsize` | String | No | `compact` | `compact` or `full` for intraday, daily and daily adjusted. |
| `max_records` | Integer | No | `50000` | Maximum observations per resource response, from 1 to 100000. |
| `max_response_bytes` | Integer | No | `8388608` | Independent limits for compressed and decoded response payloads, from 1024 to 16777216. |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connection timeout, from 1 to 300000. |
| `read_timeout_ms` | Integer | No | `10000` | TLS and response inactivity timeout, from 1 to 300000. |
| `request_timeout_ms` | Integer | No | `30000` | Total request budget per resource including pacing and retry delays, from 1 to 300000. |
| `request_interval_ms` | Integer | No | `2000` | Minimum spacing between attempts by this reader, from 1 to 60000. |
| `max_attempts` | Integer | No | `3` | Total attempts per resource including the initial request, from 1 to 10. |
| `max_requests` | Integer | No | `24` | Total first attempts and retries across all resources per reader execution, from 1 to 80. |

## Output and recovery

The fixed schema contains six string columns.

| Column | Value |
| --- | --- |
| `resource` | Selected resource name. |
| `symbol` | Equity symbol. |
| `date` | Trading date in `yyyy-MM-dd` form. Quote rows use the provider’s latest trading day. |
| `timestamp` | Original market-local time in `yyyy-MM-dd HH:mm:ss` form for intraday rows. Null elsewhere. |
| `time_zone` | Provider time zone string when present in a price series. Null for quotes. |
| `data` | Price observation or quote as JSON text. Preserves provider field names, numeric strings, exact JSON numbers and nulls. |

The connector validates each response before emitting the resource’s rows in ascending time order. A valid empty series or quote emits zero rows. Invalid responses, authentication failures, unsupported entitlements, a reported daily quota failure and exhausted retry or request budgets fail the job. The connector follows the JVM trust store for TLS and rejects redirects. API keys are omitted from source errors and checkpoint state.

The first request and all retries consume the reader’s local request budget. A restore starts a new reader with a fresh local budget. The provider’s usage on the key still applies across restores and jobs. Select resources and set `request_interval_ms` and `max_requests` for the key’s available quota. [Alpha Vantage support](https://www.alphavantage.co/support/)

A restore skips resources completed in the checkpoint. If a checkpoint records a partially emitted resource, the source fetches that resource again and verifies the contents before resuming at the saved row. A change to the saved resource snapshot fails the restore. A resource whose rows had not begun at checkpoint time is fetched afresh.

## Example

Pass the API key as a SeaTunnel job variable.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  AlphaVantage {
    token = "${ALPHA_VANTAGE_API_KEY}"
    symbol = "IBM"
    resources = ["time_series_daily", "quote"]
    outputsize = "compact"
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
