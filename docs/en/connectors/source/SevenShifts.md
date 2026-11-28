import ChangeLog from '../changelog/connector-http-7shifts.md';

# SevenShifts

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

The `SevenShifts` source exports one resource per bounded scan from the 7shifts v2 API. Install `connector-http-7shifts` on the engine. Use a customer access token with access to the selected resources and the companies and users needed to discover them. Availability depends on your 7shifts plan and access. [7shifts authentication](https://developers.7shifts.com/reference/authentication)

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes | | Customer access token, sent as Bearer authentication. |
| `resource` | String | Yes | | Resource name from the table below. |
| `base_url` | String | No | `https://api.7shifts.com` | Trusted API origin without a path, query, fragment, or credentials. An optional trailing slash is accepted. |
| `allow_insecure_http` | Boolean | No | false | Permits cleartext HTTP for an explicitly trusted, isolated endpoint. |
| `api_version` | String | No | `2026-01-01` | Date-based API version sent on every request. |
| `start_date` | String | No | | Inclusive modification cutoff, formatted as an ISO-8601 timestamp with an offset, for supported resources. |
| `page_size` | Integer | No | 100 | Requested number of rows per paginated company-scoped resource, from 1 to 100. |
| `request_timeout_ms` | Integer | No | 30000 | Total deadline, including retries and backoff, for one page, from 1 to 300000. |
| `max_attempts` | Integer | No | 4 | Maximum HTTP attempts per page, from 1 to 8. |
| common-options | | No | | [Source common options](../common-options/source-common-options.md). |

## Resources and rows

| Resource | Scope | Provider filter for `start_date` | Payload |
| --- | --- | --- | --- |
| `companies` | Token | UTC calendar date | One object per company |
| `locations`, `departments`, `roles`, `users` | Company | UTC calendar date | One object per entry |
| `time_punches`, `shifts` | Company | UTC timestamp | One object per entry |
| `wages` | User | Unavailable | Object containing current and upcoming wages |
| `assignments` | User | Unavailable | Object containing location, department, and role arrays |
| `location_assignments`, `department_assignments`, `role_assignments` | User | Unavailable | One object per assignment |

Company and user discovery follows every available cursor without applying the output date cutoff. User discovery includes active and inactive users returned by 7shifts. Scope comes from the request path and IDs are deduplicated within their discovery scope. A non-null cursor continues even when the response page is short or empty. The companies endpoint has no documented page-size parameter.

Each output row has `resource STRING`, `company_id BIGINT`, nullable `user_id BIGINT`, and `payload STRING`. The user ID comes from the user record, user route, or the entry's own user ID when present. The payload contains the complete JSON object from `data`, including nested values and exact JSON numeric precision. Exported payloads can contain personal and payroll information.

With `start_date` configured, the reader overlaps the API filter by one UTC day for date-only endpoints or one second for timestamp endpoints. It applies the exact inclusive cutoff against each returned `modified` value and keeps rows with absent or null modification timestamps. The five user-scoped endpoints require a full scan and reject `start_date`. Server defaults for other filters apply.

## Recovery and limits

Use source parallelism 1. Checkpoint state version 2 stores the traversal, page cursor, and the accepted row count plus a SHA-256 digest of the exact four output columns for a partial page; it contains neither credentials nor payload records. A restore refetches that page, reapplies the same cutoff and validates the entire accepted prefix before skipping it. A changed, reordered or missing accepted prefix fails the scan before new rows are emitted. Wages and assignments grouped objects remain single rows. The last row and the cursor or parent transition are committed together. Earlier development checkpoint formats are not supported.

Complete response validation and parent discovery run outside the collector checkpoint lock. Each poll accepts at most one prepared row before yielding, so a decoded page does not hold the lock throughout engine rate limiting. An individual blocked collector call still follows engine cancellation and checkpoint behavior. A changed or expired provider cursor can prevent resume. The API does not provide a frozen snapshot: unaccepted rows and later discovery results can change, including users returned under provider defaults. Each new scan uses the configured cutoff and current dataset. Rows accepted after the last completed checkpoint can repeat after recovery, subject to the sink's recovery semantics.

Requests use HTTPS with certificate and hostname verification by default. The source does not follow redirects or retry authentication and permission errors. It retries HTTP 429, 500, 502, 503, and 504 within the configured deadline and attempt limit. A `Retry-After` delay that does not fit the deadline fails the page. The token is supplied by the user and can be replaced when resubmitting a job. Partner OAuth is outside the connector's credential contract.

The scan fails if a response exceeds 8 MiB before or after decompression, if an endpoint exceeds 10,000 pages, or if discovery exceeds 4,096 companies or 100,000 users for one company. A response can contain at most 10,000 records. Oversized or repeated cursor tokens fail the scan.

## Example

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}

source {
  SevenShifts {
    token = ${?SEVEN_SHIFTS_TOKEN}
    resource = "time_punches"
    start_date = "2026-01-02T00:00:00Z"
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
