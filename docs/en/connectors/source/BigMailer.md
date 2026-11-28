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

import ChangeLog from '../changelog/connector-http-bigmailer.md';

# BigMailer

Install `connector-http-bigmailer` and configure the `BigMailer` source in a batch job with source parallelism 1. Each source instance reads one collection from the [BigMailer v1 API](https://docs.bigmailer.io/docs/getting-started-api). Create an API key in BigMailer and grant access to the account and selected collection.

| Resource | Records |
| --- | --- |
| `brands` | Brands visible to the account. |
| `users` | User records visible to the account. |
| `contacts` | Contacts and their fields and list memberships. |
| `lists` | Contact lists. |
| `fields` | Contact field definitions. |
| `message-types` | Message type and email preference definitions. |
| `segments` | Saved contact segments. |
| `bulk_campaigns` | Existing bulk campaigns. |
| `transactional_campaigns` | Existing transactional campaigns. |
| `suppression_lists` | Campaign suppression-list file metadata. |

The first two resources use account-wide requests. For every other resource the connector discovers all visible brand IDs, reads that resource separately within each brand, and outputs its own brand context. Brands need not be configured as a second source. All requests are GET and use only the listed collections.

The output has two `STRING` columns. `brand_id` is the record's brand for brand-scoped resources and the brand's own ID for `brands`. It is null for `users`. `data` is a JSON object with the provider's field names, nested values, numeric precision, explicit nulls, and missing fields preserved. Use `brand_id` when interpreting IDs from brand-scoped resources.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `token` | string | — | Required BigMailer API key, sent as `X-API-Key`. |
| `resource` | string | — | Required collection from the table above. |
| `page_size` | int | `100` | Requested records per page, 1–100. |
| `max_pages` | int | `2000` | Maximum pages per account-level list or per brand's child list, 1–10000. Brand discovery has its own limit. |
| `connect_timeout_ms` | int | `10000` | TCP connection timeout, 1–120000 milliseconds. |
| `read_timeout_ms` | int | `30000` | Socket read timeout, 1–120000 milliseconds. |
| `request_deadline_ms` | int | `60000` | Total request budget including the body and retries, 1–300000 milliseconds. |
| `max_attempts` | int | `4` | Maximum attempts including the first, 1–10. |
| `max_response_bytes` | int | `8388608` | Separate wire and decoded response size limits, 1024–33554432. |

Pagination follows the provider's `has_more` flag through short or empty pages. A missing or repeated continuation, permission denial, or page-limit overflow fails the scan. The source checks TLS certificate and hostname for `api.bigmailer.io` and rejects redirects. Each source instance makes one request at a time and paces dispatches. Concurrent jobs and applications may share the same account limits. Retries honor `Retry-After` within the configured deadline.

Each new job performs a bounded full read. Zeta recovery uses saved parent and data positions, scoped to the API key, resource and page size. The source checks the replayed portion of an active page before applying the saved row index. If that page, the ordered brand IDs or a continuation no longer match, recovery fails explicitly. Earlier pages are not read again. The API can change during a full read, so recovery does not provide a consistent snapshot of the account. A completed resource stays complete after restore. Start a new scan if the API key changes.

## Example

```hocon
env {
    job.mode = "BATCH"
    parallelism = 1
}
source {
    BigMailer {
        token = ${BIGMAILER_TOKEN}
        resource = "contacts"
        page_size = 100
    }
}
sink {
    Console {}
}
```

<ChangeLog />
