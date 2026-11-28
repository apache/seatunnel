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

import ChangeLog from '../changelog/connector-http-boldsign.md';

# BoldSign

Install `connector-http-boldsign` and configure `BoldSign` as a batch source with parallelism 1. Each source instance reads one collection from the [BoldSign API](https://developers.boldsign.com/api-overview/getting-started/). Use a key issued for the [account's data region](https://support.boldsign.com/kb/article/14423/resolving-401-unauthorized-error).

| Resource | Records |
| --- | --- |
| `documents` | Metadata for documents visible in My Documents. |
| `brands` | All brands visible to the key. |
| `senderIdentities` | Sender identities visible to the key. |
| `teams` | Team records. |
| `templates` | Template metadata. |
| `users_list` | Account user records. |
| `custom_fields` | Custom fields for each discovered brand. |
| `contacts` | Visible contacts, using the provider's default contact scope. |

The connector uses GET to read list data. It automatically discovers brands for `custom_fields`. Every row has two `STRING` fields. `brand_id` is populated for custom fields and null for other resources. `data` is the JSON object returned by BoldSign, with field names, nested values, numeric precision, explicit nulls, and missing fields preserved.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `token` | string | — | Required BoldSign API key, sent via `X-API-KEY`. |
| `resource` | string | — | Required resource name from the table above. |
| `region` | string | `us` | Account location. Accepted values are `us`, `eu`, `ca` and `au`. |
| `start_date` | string | — | Optional inclusive creation-time lower bound for documents, teams, templates and users. |
| `end_date` | string | Job initialization time when `start_date` is set | Optional inclusive creation-time upper bound for the same resources. |
| `page_size` | int | `50` | Requested page size, 1–100. For documents it must divide 10,000 evenly. |
| `max_pages` | int | `2000` | Maximum pages per list, including document continuation windows, 1–20000. |
| `max_unpaged_records` | int | `10000` | Maximum rows per brand or custom field response, 1–50000. |
| `connect_timeout_ms` | int | `10000` | TCP connection timeout, 1–120000 milliseconds. |
| `read_timeout_ms` | int | `30000` | Socket read timeout, 1–120000 milliseconds. |
| `request_deadline_ms` | int | `60000` | Total request budget including retries, 1–300000 milliseconds. |
| `max_attempts` | int | `4` | Maximum attempts per request, 1–10. |
| `max_response_bytes` | int | `8388608` | Separate transfer and decoded limits, 1024–33554432 bytes. |

Dates use ISO 8601 whole seconds with an explicit offset. When either date is set, the connector filters locally on the returned `createdDate` epoch seconds. All provider results still participate in pagination. A missing creation timestamp in an explicitly filtered read fails because the record cannot be placed in the window.

The source normally uses numbered pages. After a full 10,000-record document window it continues with `NextCursor`, replaying the trailing cursor group so records sharing a cursor can be read without repeating already emitted IDs. If the API no longer returns the saved overlap, or a whole window has the same cursor and cannot advance, the scan fails explicitly. Brand and custom-field collections are single responses.

Zeta checkpoints store the frozen creation window and active request positions. Recovery verifies the active page or brand list before using saved offsets. Changing the API key, region, resource, page size or date options requires a fresh read. Each new scan uses the provider's live visibility and records. The API does not provide a consistent snapshot or report deletes through this source. The source validates TLS and rejects redirects.

## Example

```hocon
env {
    job.mode = "BATCH"
    parallelism = 1
}
source {
    BoldSign {
        token = ${BOLDSIGN_API_KEY}
        region = "us"
        resource = "documents"
        start_date = "2025-01-01T00:00:00Z"
    }
}
sink {
    Console {}
}
```

<ChangeLog />
