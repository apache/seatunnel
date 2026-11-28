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

import ChangeLog from '../changelog/connector-http-appcues.md';

# Appcues

Appcues reads experience and account definitions from the Appcues Public API. Run in batch mode with source parallelism 1. The connector reads the seven account resources listed below over verified HTTPS and HTTP Basic authentication. [Appcues Public API](https://api.appcues.com/v2/docs)

## Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## Resources

Each resource is retrieved as a JSON array from `GET /v2/accounts/{account_id}/{resource}`. The connector applies the date window locally to the resource-specific field.

| Resource | Time field | Accepted formats |
| --- | --- | --- |
| `launchpads` | `updated_at` | Unix epoch milliseconds |
| `flows` | `created_at` | ISO 8601 or Unix epoch milliseconds |
| `banners` | `updated_at` | Unix epoch milliseconds |
| `checklists` | `created_at` | ISO 8601 or Unix epoch milliseconds |
| `pins` | `updated_at` | Unix epoch milliseconds |
| `tags` | `updated_at` | ISO 8601 |
| `segments` | `updated_at` | ISO 8601 |

Milliseconds may be represented as integral JSON numbers or decimal strings. ISO 8601 accepts timestamps with or without fractional seconds. The time window includes `start_date` and excludes `end_date`. A missing or null time field skips that record. An invalid present time value fails the resource before any of its rows are emitted. The full response is subject to `max_records` before selection.

## Source options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `username` | String | Yes | — | Appcues API key used as the Basic username. |
| `password` | String | Yes | — | Appcues API secret used as the Basic password. |
| `account_id` | String | Yes | — | Appcues account identifier. |
| `start_date` | String | Yes | — | Inclusive start of the local time window, as ISO 8601 with a timezone. |
| `end_date` | String | No | Initial scan time | Exclusive end of the window, as ISO 8601 with a timezone. |
| `resources` | List of strings | No | All seven in the order above | Ordered list of distinct resource names. |
| `api_url` | String | No | `https://api.appcues.com` | US or EU HTTPS API origin. The EU origin is https://api.eu.appcues.com. |
| `max_records` | Integer | No | `50000` | Maximum entries per resource response before filtering, from 1 to 100000. |
| `max_response_bytes` | Integer | No | `16777216` | Separate encoded and decoded response byte limits, from 1024 to 33554432. |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connection timeout, from 1 to 300000. |
| `read_timeout_ms` | Integer | No | `10000` | TLS and response inactivity timeout, from 1 to 300000. |
| `request_timeout_ms` | Integer | No | `30000` | Total timeout per resource including retry delays, from 1 to 300000. |
| `max_attempts` | Integer | No | `4` | Total attempts per resource including the initial attempt, from 1 to 10. |

## Output and restore

Output contains four string columns.

| Column | Value |
| --- | --- |
| `resource` | Resource name. |
| `id` | Provider record identifier. |
| `cursor_time` | Time normalized to UTC ISO 8601 for filtering. |
| `data` | Full original record as JSON text with nested fields, nulls and precise numbers. |

Records within each resource are ordered by normalized time and ID. The read scans each complete resource array. Pagination, partial responses and unexpected continuation signals fail the job. For separate jobs, configure adjacent time windows to read additional history.

The job freezes the end of the window before reading. Checkpoint restore uses that same window, skips completed resources and refetches a pending response. For partially emitted resources, the source compares the selected contents with the recorded digest before restoring the row position. Provider records that have already been read may change independently.

The connector uses the JVM TLS trust store and checks the configured regional hostname. Redirects and permanent authentication failures fail the job. Temporary HTTP 429, 500, 502, 503 and 504 responses can retry within per-resource attempts and deadline. Credentials are omitted from source errors and checkpoint state.

## Example

Pass the API key and secret as SeaTunnel job variables.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Appcues {
    username = "${APPCUES_API_KEY}"
    password = "${APPCUES_API_SECRET}"
    account_id = "my-account"
    resources = ["flows", "segments"]
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-02-01T00:00:00Z"
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
