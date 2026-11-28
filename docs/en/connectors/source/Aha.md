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

import ChangeLog from '../changelog/connector-http-aha.md';

# Aha

The `Aha` source reads Aha! Roadmaps and Ideas records visible to a credential through the [Aha v1 REST API](https://www.aha.io/api). It performs a bounded full refresh. Set `job.mode` to `BATCH` and `parallelism` to `1`.

## Resources

| Resource | Path under `/api/v1` | Parent discovery |
| --- | --- | --- |
| `features` | `/features` | — |
| `products` | `/products` | — |
| `idea_categories` | `/products/{product_id}/idea_categories` | Pages through products |
| `ideas` | `/ideas` | — |
| `idea_endorsements` | `/ideas/{idea_id}/endorsements` | Pages through ideas |
| `idea_comments` | `/ideas/{idea_id}/idea_comments` | Pages through ideas |
| `users` | `/users` | — |
| `goals` | `/goals` | — |

Each selected child resource discovers every visible parent page independently, even when its parent resource is absent from `resources`. Discovery rows are emitted only when the parent resource is selected separately.

Each row contains four string columns. `resource` is the configured resource name, `record_id` is the provider ID, `parent_id` is null for top-level resources, and `data` holds the full provider object as JSON. JSON field names, nested values, nulls, and precise numbers are preserved. Apply your data handling policy to the destination because provider objects can contain user and idea content.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `api_url` | String | Yes | — | Account origin such as `https://company.aha.io`. |
| `token` | String | Yes | — | API key or an issued OAuth 2 access token, without the `Bearer` prefix. |
| `resources` | List\<String\> | No | All eight | Resource names, scanned in the listed order. Use each name once. |
| `page_size` | Integer | No | 30 | Requested records per page, from 1 to 200. |
| `max_pages` | Integer | No | 10000 | Maximum pages per direct collection, parent scan, or child collection, from 1 to 20000. |
| `max_parents` | Integer | No | 10000 | Maximum discovered parents per selected child resource, from 1 to 20000. |
| `max_response_bytes` | Integer | No | 4194304 | Separate limits for compressed and decoded entity bytes, from 1024 to 16777216. |
| `connect_timeout_ms` | Integer | No | 10000 | TCP connection timeout in milliseconds, from 1 to 300000. |
| `read_timeout_ms` | Integer | No | 10000 | TLS and response idle timeout in milliseconds, from 1 to 300000. |
| `request_timeout_ms` | Integer | No | 30000 | Total per-page budget in milliseconds, including retries and retry waits, from 1 to 300000. |
| `max_attempts` | Integer | No | 3 | Maximum attempts per page, including the first, from 1 to 10. |

Use the account's HTTPS `tenant.aha.io` origin, with a port only when required. Enter the origin without an API path, credentials, query string, or fragment. Keep the token out of shared job configs. The connector uses the token's existing account permissions and accepts either a personal API key or an already-issued OAuth access token. OAuth token renewal is managed outside the job.

## Pagination and recovery

The connector builds v1 requests using consecutive page numbers and reads `current_page`, `total_pages`, and `total_records` from the response. It checks the entire bounded page before emitting records. Truncated responses, inconsistent metadata, repeated pages, expired or unauthorized credentials, and inaccessible routes fail the job. HTTP 429, 500, 502, 503, and 504 can be retried within the same page deadline. The connector honors the provider rate-limit reset and standard retry delays within that deadline. It verifies the TLS certificate and never follows redirects or response-supplied links.

A checkpoint resumes at the first unfinished page using the same account origin, ordered resource list, and page size. The connector can re-fetch that page after recovery. Aha v1 page-number reads can observe concurrent changes in Aha. An immutable remote snapshot and incremental cursor are unavailable.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Aha {
    api_url = "https://company.aha.io"
    token = "${aha_token}"
    resources = ["products", "idea_categories", "ideas", "idea_comments"]
    page_size = 30
  }
}

sink {
  Console {}
}
```

<ChangeLog />
