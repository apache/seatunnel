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

import ChangeLog from '../changelog/connector-http-woocommerce.md';

# WooCommerce

> Read orders from one WooCommerce store.

## Description

Reads the authenticated WooCommerce REST API `GET /wp-json/wc/v3/orders`. This is a bounded batch extraction of orders created within an explicit UTC window, not CDC, incremental synchronization, or a transactionally consistent snapshot. No orders are created or modified.

## Key Features

- [x] Batch
- [ ] Streaming
- [ ] Exactly-once

Parallelism must be 1. Recovery replays the entire configured window; no remote page offset is checkpointed. A failed job may already have emitted records to a nontransactional sink. Use an idempotent sink keyed by store and order ID when replay matters.

## Prerequisites

Create a WooCommerce REST API key with **Read** permission for a user allowed to view orders. Use HTTPS and a valid server certificate. Install private CA certificates through the JVM truststore on every worker when needed. Redirects are not followed: configure the final HTTPS store URL, including a WordPress installation subdirectory if applicable. WordPress must forward the Authorization header to WooCommerce.

This connector uses Basic authentication with the consumer key and secret, not WordPress login credentials or the unauthenticated Store API. Keep HTTP wire/header logging disabled. Native parsed-config masking covers both credentials; use environment substitution or a secret provider and never include credentials in the URL.

See [REST authentication](https://developer.woocommerce.com/docs/apis/rest-api/authentication/) and the [orders API](https://developer.woocommerce.com/docs/apis/rest-api/v3/orders/).

## Read Contract

The request sends `after=start_date`, `before=end_date`, `dates_are_gmt=true`, `orderby=id`, `order=asc`, and `status=any`. Both creation-time bounds are exclusive. Adjacent nonoverlapping windows can omit orders exactly on a boundary; overlap windows and deduplicate by store/order ID if composing exports. Creation-time filtering does not capture later updates or deletes.

Pagination starts at page 1. Each response must contain `X-WP-Total`, `X-WP-TotalPages` and the expected number of order objects. Missing or inconsistent pagination, changed totals, repeated/decreasing IDs, malformed JSON, invalid IDs and `max_pages` overflow fail the job instead of returning a truncated successful result. Empty windows complete with zero rows.

Stable totals and ascending IDs cannot detect every concurrent edit or same-count replacement. Read a quiescent historical window for repeatability; no snapshot-completeness guarantee is made.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| url | STRING | yes | - | Final HTTPS store URL without credentials, query or fragment |
| consumer_key | STRING | yes | - | Read-only REST consumer key |
| consumer_secret | STRING | yes | - | REST consumer secret |
| start_date | STRING | yes | - | Exclusive creation lower bound, ISO8601 with seconds and offset |
| end_date | STRING | yes | - | Exclusive creation upper bound; must follow start_date |
| schema | CONFIG | yes | - | Selected order fields and SeaTunnel types |
| page_size | INT | no | 100 | Orders per page, 1 to 100 |
| max_pages | INT | no | 10000 | Maximum pages, 1 to 1000000; exceeding it fails before emitting the first page |
| decimal_places | INT | no | 2 | WooCommerce `dp` parameter, 0 to 18 |
| max_retries | INT | no | 3 | Additional attempts per page, 0 to 5 |
| retry_delay_ms | INT | no | 1000 | Minimum retry wait, 1 to 60000 ms |
| request_timeout_ms | INT | no | 30000 | Per-attempt connect/read timeout and request-abort deadline, 1 to 120000 ms |
| max_response_bytes | INT | no | 8388608 | Decompressed page size limit, 1024 to 16777216 bytes |

The engine's default FAIL_FAST failure policy is supported; table-skipping policies are not applicable to this single-table source.

HTTP 429, 500, 502, 503, 504 and transport failures are retried within the budget. Other statuses fail. Numeric and HTTP-date Retry-After values up to 60 seconds are honored; invalid or longer waits fail so the job can be retried later. Closing the reader aborts active requests and wakes retry waits. The deadline bounds HTTP I/O attempts, not the whole job: JVM DNS resolution, JSON parsing, retry waits and a blocked downstream collector are outside a strict wall-clock job guarantee.

## Data Types

Define only fields required by the pipeline. JSON conversion uses SeaTunnel's existing schema mapping, including nested ROW, MAP and supported ARRAY types. Missing or null fields are null. Unknown response fields are ignored.

Select monetary fields as DECIMAL, not FLOAT/DOUBLE. Returned amounts must fit the declared precision/scale exactly; the connector fails rather than rounding. However, WooCommerce itself formats amounts according to `decimal_places` before sending them. Set this option to the precision required by the store; the default 2 is not a promise to preserve higher-precision stored values.

Nested order arrays such as `line_items` can be preserved as a STRING containing JSON or `array<map<string,string>>`. The current common schema parser does not support ARRAY&lt;ROW&gt;; this connector does not change that contract. Customer addresses, notes and metadata may contain personal information: use access-controlled sinks and avoid logging output rows.

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  WooCommerce {
    url = "https://shop.example.com"
    consumer_key = ${WOOCOMMERCE_CONSUMER_KEY}
    consumer_secret = ${WOOCOMMERCE_CONSUMER_SECRET}
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-02-01T00:00:00Z"
    decimal_places = 3
    schema {
      fields {
        id = bigint
        status = string
        currency = string
        total = "decimal(18,3)"
        billing = { country = string }
        line_items = "array<map<string,string>>"
      }
    }
  }
}
sink {
  LocalFile {
    path = "/data/orders"
    file_format_type = "json"
  }
}
```

## Changelog

<ChangeLog />
