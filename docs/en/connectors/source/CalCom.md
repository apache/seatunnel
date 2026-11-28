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

import ChangeLog from "../changelog/connector-http-cal-com.md";

# Cal.com

## Description

The Cal.com source reads six resources from `https://api.cal.com` in a bounded batch. Use a personal API key from Cal.com. The records available depend on that key and its account. [Cal.com API v2](https://cal.com/docs/api-reference/v2/introduction).

| Resource | Endpoint | Record key | Read behavior |
| --- | --- | --- | --- |
| `event_types` | `/v2/event-types` | `id` | Grouped by default. The current version returns a flat list. |
| `my_profile` | `/v2/me` | `id` | One user profile. |
| `schedules` | `/v2/schedules` | `id` | Complete list at version `2024-06-11`. |
| `calendars` | `/v2/calendars` | `externalId` | Calendars within each connected integration. |
| `bookings` | `/v2/bookings` | `id` | Opaque cursor or validated legacy offset pagination. |
| `conferencing` | `/v2/conferencing` | `id` | Installed conferencing application metadata. |

The default event types version `2024-04-15` yields groups. Choose `2026-06-12` to use the current flat event list. The source reads calendars inside each connected integration and keeps the parent metadata alongside each calendar. The conferencing list uses the native public metadata contract, dropping any unexpected secret key or token fields. [Event types](https://cal.com/docs/api-reference/v2/event-types/list-event-types), [calendars](https://cal.com/docs/api-reference/v2/calendars/get-all-calendars) and [conferencing](https://cal.com/docs/api-reference/v2/conferencing/list-your-conferencing-applications).

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| token | string | yes | | Personal API key beginning with `cal_`. |
| resources | list | no | [] | Resource names from the table. An empty list selects all six. |
| event_types_api_version | string | no | 2024-04-15 | Grouped `2024-04-15` or flat `2026-06-12`. |
| bookings_api_version | string | no | 2026-05-01 | Cursor-based `2026-05-01` or offset-based `2024-08-13`. |
| bookings_page_size | int | no | 100 | Number of bookings requested per page, from 1 to 100. |
| max_items_per_response | int | no | 10000 | Maximum records in a full-list response. Applies to the flattened result for nested lists. |
| max_pages_per_resource | int | no | 10000 | Maximum page count for a resource. |
| tls_trust_certificate | string | no | JVM trust | Optional source-local PEM or base64 DER X.509 trust anchor. Hostname verification stays active. |
| connect_timeout_ms | int | no | 10000 | TCP connect timeout in milliseconds. |
| request_timeout_ms | int | no | 30000 | Deadline for one HTTPS attempt. |
| operation_timeout_ms | int | no | 60000 | Deadline across one read's attempts and waits. Must be at least `request_timeout_ms`. |
| request_interval_ms | int | no | 600 | Minimum spacing between attempts in milliseconds. |
| max_retries | int | no | 3 | Additional attempts for transient read failures. |
| max_retry_delay_ms | int | no | 30000 | Maximum permitted Retry-After or backoff delay. |
| max_response_bytes | int | no | 8388608 | Maximum response size before and after decompression. |

## Paging and checkpoints

Bookings `2026-05-01` returns `pagination.hasMore` and `pagination.nextCursor`. The source sends the cursor as an opaque query parameter and continues through short or empty pages with an advertised continuation. Bookings `2024-08-13` uses `skip` and `take`. The source validates the response's offset metadata before advancing by the requested page size. Other resources use their documented full responses. [Bookings API](https://cal.com/docs/api-reference/v2/bookings/get-all-bookings) and [schedules API](https://cal.com/docs/api-reference/v2/schedules/get-all-schedules).

A checkpoint saves the resource, current page and emitted row position. Restore revalidates the page digest and resumes at the next row. A changed page, expired cursor, or changed API key, version or selection can require a fresh batch. Cal.com can change data while a scan is running. A new batch reads the current data visible to the configured key.

The source reports authentication errors, redirects, unsupported response shapes, invalid pagination and API errors carried in an HTTP 200 response. Transient failures can be retried within the configured deadline.

## Output

All four columns use string types.

| Column | Meaning |
| --- | --- |
| `resource` | Selected resource name. |
| `id` | Exact provider identifier, with numeric IDs expressed as strings. |
| `parent` | JSON metadata for the event-type group or calendar connection with child arrays removed. Null for other rows. |
| `record` | JSON record with native field names, nested data, exact numbers and nulls. |

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  CalCom {
    token = ${CAL_COM_TOKEN}
    resources = ["event_types", "calendars", "bookings"]
  }
}
```

## Changelog

<ChangeLog />
