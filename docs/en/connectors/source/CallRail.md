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

# CallRail Source

`CallRail` reads the CallRail v3 API in a bounded scan of one account at source parallelism 1. Install `connector-http-callrail` if it is absent from your distribution.

| Resource | API operation | Output timestamp |
| --- | --- | --- |
| `calls` | `GET /v3/a/{account_id}/calls.json` | `start_time` |
| `conversations` | `GET /v3/a/{account_id}/text-messages.json` | `last_message_at` |
| `users` | `GET /v3/a/{account_id}/users.json` | `created_at` |
| `companies` | `GET /v3/a/{account_id}/companies.json` | `created_at` |

The source sends `Authorization: Token token="<token>"`. The token owner's CallRail permissions determine which account records are available. Calls and conversations request documented supplemental attribution, formatting and message fields. Legacy cookie fields `utma`, `utmb`, `utmc`, `utmv` and `utmz` are preserved if returned. Current field-selection documentation does not list these keys. Embedded recordings, scripts, message media and other URLs remain data. [CallRail API](https://apidocs.callrail.com/) · [Authorization](https://apidocs.callrail.com/#authorization)

## Options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `token` | Yes | | Personal CallRail v3 API key. |
| `account_id` | Yes | | Account ID, supplied as a string. Numeric and masked alphanumeric IDs are accepted. |
| `resources` | Yes | | Non-empty ordered list of unique resource names from the table. |
| `start_date` | Yes | | Inclusive UTC date or offset-bearing RFC 3339 timestamp. A date is midnight UTC. |
| `end_date` | No | Scan start | Exclusive UTC date or offset-bearing RFC 3339 timestamp. |
| `calls_pagination` | No | `relative` for numeric account IDs, `offset` for masked account IDs | Choose relative calls or numbered offset pages. Other resources always use numbered pages. |
| `page_size` | No | 100 | Number of records requested per page, from 1 to 250. |
| `max_pages_per_scope` | No | 1000 | Maximum pages per resource and date slice, from 1 to 10000. |
| `max_requests` | No | 100000 | Maximum successfully processed responses in a scan, from 1 to 1000000. |
| `max_attempts` | No | 4 | Total attempts per request, from 1 to 10. |
| `request_timeout_ms` | No | 30000 | Total acquisition budget for one page, from 100 to 300000 milliseconds. |
| `max_response_bytes` | No | 8388608 | Decoded response limit, from 1024 to 67108864 bytes. |
| `endpoint` | No | `https://api.callrail.com` | Bare HTTPS origin. An explicit custom origin requires `trust_certificate`. |
| `trust_certificate` | No | | Worker-local X.509 PEM path for the custom HTTPS origin. |

## Output

The `callrail.records` table contains five `STRING` columns.

| Column | Description |
| --- | --- |
| `resource` | Resource name. |
| `record_id` | Exact provider ID, including masked strings or legacy numeric IDs. |
| `account_id` | Account scope from the source configuration. |
| `record_time` | Provider timestamp normalized to UTC. User or company records with no creation time have a null value. |
| `payload` | Complete provider record as JSON text with nested values, unknown fields and numeric precision preserved. |

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  CallRail {
    token = ${CALLRAIL_API_TOKEN}
    account_id = "227799611"
    resources = ["calls", "conversations", "users", "companies"]
    start_date = "2025-01-01"
    end_date = "2025-04-15"
  }
}

sink {
  Console {}
}
```

## Time windows, pagination and recovery

Calls and conversations request inclusive calendar dates in `Etc/UTC`, in slices of up to 100 days. The source applies `[start_date, end_date)` locally with fractional timestamp precision. Users and companies are read once through all available pages. Their creation-time window is applied locally, and records with no creation timestamp are retained. Visibility depends on CallRail's retention and access rules. [Filtering by date](https://apidocs.callrail.com/#filtering-data-by-date)

Numbered mode uses `page`, `per_page`, `total_pages` and `total_records` from each response. Relative calls follow the server's `next_page` when `has_next_page` is true. Each continuation must preserve the HTTPS origin, account route, date slice, fields, page size and relative mode. This includes the vendor's extensionless calls URL variant. Use offset calls with a masked account if the provider returns a continuation with a differing internal account identifier. An empty or short response can still have a subsequent page. [Offset and relative pagination](https://apidocs.callrail.com/#relative-pagination) · [Offset pagination](https://apidocs.callrail.com/#offset-pagination)

A checkpoint retains the frozen end time, resource position, current slice, pagination state, page fingerprint and published row count. Recovery verifies the raw page and its continuation before emitting any saved partial suffix. Changing the token, account, resource order, time window, paging options, endpoint or trust path requires a fresh scan. The provider lists do not offer a read-isolated snapshot or deletion log.

The HTTPS client verifies the certificate and hostname, forbids redirects and bounds retries and decoded responses. It honors a valid `Retry-After` delay and applies exponential backoff to 429 when the provider supplies no delay. CallRail can limit reads across the entire account. [Rate limiting](https://apidocs.callrail.com/#rate-limiting)
