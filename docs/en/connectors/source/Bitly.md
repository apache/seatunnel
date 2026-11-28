import ChangeLog from '../changelog/connector-http-bitly.md';

{/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */}

# Bitly

Install `connector-http-bitly` and set the source name to `Bitly`. The connector reads the Bitly v4 API over HTTPS with a Bearer token. It emits a bounded table named `records` with source parallelism 1. [Bitly API reference](https://dev.bitly.com/api-reference/)

## Resources

| Resource | Rows | Time selection |
| --- | --- | --- |
| `bsds` | Custom-domain response object | Current list |
| `campaigns`, `channels` | One row per campaign or channel | `modified` |
| `organizations`, `groups` | One row per organization or group | `modified` |
| `group_preferences` | One preference object per group | Current preferences |
| `organization_shorten_counts`, `group_shorten_counts` | One metrics response per organization or group | Historical day bins at the configured end |
| `bitlinks` | One row per Bitlink per group, including archived links | `created_at` |
| `bitlink_clicks` | One clicks response per Bitlink per group | Historical day bins at the configured end |
| `qr_codes` | QR codes associated with a Bitlink | `modified` |

Every row contains `resource`, `record_id`, `organization_id`, `group_id`, `bitlink_id` and `payload`. All columns are strings. Parent identifiers can be null when no relation exists or Bitly does not report it. The payload is the full provider JSON object. Use the resource, applicable parent identifiers and record ID together to identify rows.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | STRING | Yes | | Bitly API token. |
| `resources` | LIST of STRING | Yes | | Resource names above, each selected once. |
| `start_date` | STRING | Yes | | Inclusive RFC 3339 window start with an offset. |
| `end_date` | STRING | Yes | | Exclusive RFC 3339 window end with an offset. |
| `qr_mode` | STRING | No | `stored` | Read stored QR codes through current v4 endpoints. `legacy_bitlink` uses the older one-GET-per-Bitlink API. |
| `page_size` | INT | No | 50 | Requested size for each Bitlink or stored QR page. Range 1 to 100. |
| `max_pages_per_parent` | INT | No | 1000 | Maximum pages per parent and paginated resource. Range 1 to 5000. |
| `max_requests` | INT | No | 50000 | Maximum logical GETs per scan. Retries and replays of unfinished requests do not consume another logical position. Range 1 to 1000000. |
| `max_attempts` | INT | No | 4 | Total attempts per eligible GET. Range 1 to 10. |
| `request_timeout_ms` | INT | No | 30000 | Total acquisition budget, including retries and the optional QR detail/image pair. Range 100 to 300000. |
| `max_response_bytes` | INT | No | 8388608 | Maximum bytes of a decoded successful response. Range 1024 to 67108864. |
| `endpoint` | STRING | No | `https://api-ssl.bitly.com` | Custom HTTPS origin without path, query, fragment or credentials. Requires `trust_certificate`. |
| `trust_certificate` | STRING | No | | Worker-local PEM trust anchor for the custom origin. Hostname checks stay enabled. |

## Scope and recovery

The token's organization access controls discovery. Organization and group requests include parent records outside the output window so the connector can still find their descendants. A Bitlink-only read uses the API's epoch-second creation filters and then applies the exact configured interval. A scan that needs links for child reads discovers all links and applies the creation window only to `bitlinks` output. Metrics requests explicitly use `unit=day` and `units=-1`, anchored at `end_date` truncated to a second. The source retains Bitly's metric bins, including historical bins outside the record window.

Stored QR scans enumerate a group's coupled codes, verify the detail's group and Bitlink IDs, and read the SVG image as inline JSON. The output wraps both responses under `metadata` and `image`. Legacy mode preserves the old response object and requires an account that can use the older route. The connector only reads existing provider responses. URLs and QR image data are included as data and are never followed.

A verified `search_after` value resumes the current parent. A continuation URL must match the current origin, path and any supplied fixed query settings. A page with zero rows can still have a continuation. Repeated pages and cursors, contradictory parents, permission failures and incomplete scans at configured limits fail the job. The connector holds at most 10,000 outstanding descendant requests. Checkpoints preserve the pending parent and continuation, and a partial response must match the saved fingerprint before publication resumes. The API does not isolate changes made while the source reads the account. This is a bounded scan with date selection, and metric responses remain aggregated snapshots.

Monthly API quota errors fail immediately. Bitly platform throttling retries only when the response identifies the platform limit and supplies a positive `Retry-After`. No redirects are followed. [Bitly rate limits](https://dev.bitly.com/docs/getting-started/rate-limits/)

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Bitly {
    token = ${BITLY_TOKEN}
    resources = ["bitlinks", "bitlink_clicks", "qr_codes"]
    start_date = "2024-03-01T00:00:00Z"
    end_date = "2024-04-01T00:00:00Z"
  }
}
```

## Changelog

<ChangeLog />
