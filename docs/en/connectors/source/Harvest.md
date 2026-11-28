<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

import ChangeLog from '../changelog/connector-http-harvest.md';

# Harvest

## Support Those Engines

SeaTunnel Zeta, Flink, Spark

## Description

Reads time entries from one Harvest account using `GET /v2/time_entries`. This source supports BATCH mode and source parallelism 1. The token's permissions determine which entries are visible; it does not elevate account access.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| token | String | yes | - | Personal access token, without `Bearer `. Uses the engine's existing `token` log masking. |
| account_id | String | yes | - | Numeric Harvest account ID accessible to this token. |
| schema | Config | yes | - | Schema for fields in each time entry. Nested objects use ROW types; only configured fields are emitted. |
| base_url | String | no | `https://api.harvestapp.com` | HTTP(S) origin without user info, query, fragment or path. Override only for a trusted proxy or testing. Plain HTTP sends credentials in clear text. Redirects are rejected. |
| page_size | Int | no | 100 | Entries per page, 1–2000. |
| from | String | no | - | Inclusive lower `spent_date`, in `yyyy-MM-dd` format. |
| to | String | no | - | Inclusive upper `spent_date`, in `yyyy-MM-dd` format; must not precede `from`. |
| updated_since | String | no | - | ISO 8601 timestamp with offset, e.g. `2026-11-01T00:00:00Z`. Filters updates; it is not a persisted incremental cursor. |
| retry | Int | no | 3 | Additional attempts per page for HTTP 429, 500, 502, 503 and 504; 0 disables retries. |
| retry_backoff_ms | Int | no | 1000 | Positive initial exponential retry delay in milliseconds. |
| max_retry_delay_ms | Int | no | 60000 | Maximum wait per retry; must be at least `retry_backoff_ms`. A longer provider delay fails the page. |
| connect_timeout_ms | Int | no | 10000 | Positive connection and connection-pool timeout in milliseconds. |
| socket_timeout_ms | Int | no | 60000 | Positive socket read timeout in milliseconds. |
| common options | - | no | - | See [Source Common Options](../common-options/source-common-options.md). Source parallelism must be 1. |

## Reading and recovery

The source requests pages sequentially and stops only when the response explicitly contains `next_page: null`. It validates the `time_entries` array, the current page and the consecutive next page before emitting data. Malformed responses, invalid paging metadata, schema conversion failures and HTTP errors fail the job; they are not treated as an empty result. No response URL is followed.

Only the status codes listed above are retried. Authentication errors, redirects, malformed JSON, conversion errors and network exceptions are not retried by this connector. A valid `Retry-After` (seconds or HTTP date) is combined with exponential backoff by taking the longer delay. Missing or malformed values use local backoff. A delay above `max_retry_delay_ms` fails rather than retrying earlier than requested. Interruption stops retries.

A checkpoint stores only a versioned completion flag. An unfinished split replays from page 1 after recovery, so already emitted entries may be emitted again; use the time-entry `id` with an idempotent/upsert sink when needed. A completed split is not reread. Neither page offsets nor tokens are stored in reader split state. Credentials still belong to the job configuration and must be protected accordingly.

Harvest's paginated API is not a consistent snapshot: concurrent inserts, updates and deletes can change pages, and a time filter does not freeze them. This source does not provide CDC, delete events or exactly-once extraction. For stable exports, read a period that is no longer changing and reconcile against the service as needed. Network requests and retry waits do not hold the checkpoint lock.

## Example

```hocon
env { parallelism = 1, job.mode = "BATCH" }
source {
  Harvest {
    token = "<personal-access-token>"
    account_id = "12345"
    from = "2026-11-01"
    to = "2026-11-23"
    schema {
      fields {
        id = bigint
        spent_date = date
        hours = "decimal(18,4)"
        notes = string
        user = "row<id bigint, name string>"
      }
    }
  }
}
sink { Console {} }

```

## References

- [Harvest time entries](https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/)
- [Harvest authentication](https://help.getharvest.com/api-v2/authentication-api/authentication/authentication/)
- [Harvest rate limits](https://help.getharvest.com/api-v2/introduction/overview/general/)

<ChangeLog />
