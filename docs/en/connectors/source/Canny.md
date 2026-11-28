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

import ChangeLog from "../changelog/connector-http-canny.md";

# Canny

The Canny source reads the resources visible to a workspace API key through read-only JSON POST requests to https://canny.io. Select the collections to export or leave the selection empty to read all ten. [Canny API reference](https://developers.canny.io/api-reference).

| Resource | API list path | Pagination |
| --- | --- | --- |
| `boards` | `/api/v1/boards/list` | One complete board list |
| `categories` | `/api/v1/categories/list` | Offset for each discovered board |
| `changelog_entries` | `/api/v1/entries/list` | Changelog offset |
| `comments` | `/api/v2/comments/list` | Cursor |
| `companies` | `/api/v2/companies/list` | Cursor |
| `posts` | `/api/v1/posts/list` | Offset |
| `status_changes` | `/api/v2/status_changes/list` | Cursor |
| `tags` | `/api/v1/tags/list` | Offset |
| `users` | `/api/v2/users/list` | Cursor |
| `votes` | `/api/v2/votes/list` | Cursor |

A category-only scan discovers boards without emitting board rows. All other selected collections are read once at their native workspace scope. Pagination follows the server's continuation flag, even after a short or empty page. The changelog request starts with `limit=1000` and `skip=0`, then uses `limit=10000` and `skip=1000` if needed. A continuation after the second page fails explicitly because the API permits a maximum offset of 1000.

## Output

| Column | Type | Description |
| --- | --- | --- |
| `resource` | STRING | Selected resource name |
| `id` | STRING | Native record ID |
| `board_id` | STRING | Owning board ID when available. Populated from board discovery for categories |
| `post_id` | STRING | Related post ID when available. Equals `id` for posts |
| `record` | STRING | Native JSON object, including nested data and explicit JSON nulls |

Use `resource`, `id`, and `board_id` together when downstream processing distinguishes categories across boards. Raw records may contain personal information.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes | | Workspace API key, passed as `apiKey` in the HTTPS request body |
| `resources` | List of strings | No | `[]` | Use the names from the resource table. An empty list selects all ten |
| `tls_trust_certificate` | String | No | | Optional source-local PEM or base64 DER X.509 trust anchor. HTTPS hostname verification is always enabled |
| `page_size` | Integer | No | `100` | Rows requested per page from 1 to 100, excluding boards and changelog entries |
| `max_pages_per_collection` | Integer | No | `10000` | Maximum pages in one resource or one board's categories, from 1 to 100000 |
| `max_pages_per_scan` | Integer | No | `100000` | Maximum accepted responses per scan, from 1 to 1000000 |
| `max_unpaginated_items` | Integer | No | `10000` | Maximum boards in the complete list, from 1 to 100000 |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connect timeout, from 100 to 300000 ms |
| `request_timeout_ms` | Integer | No | `30000` | Deadline per HTTPS attempt, from 100 to 300000 ms |
| `operation_timeout_ms` | Integer | No | `120000` | Deadline per page, including pacing, retries and waits. At least `request_timeout_ms` and at most 600000 ms |
| `request_interval_ms` | Integer | No | `3700` | Minimum time between attempts, from 0 to 60000 ms. The default allows for the Free plan's hourly limit on a single reader |
| `max_retries` | Integer | No | `3` | Additional attempts for eligible transient failures, from 0 to 8 |
| `max_retry_delay_ms` | Integer | No | `60000` | Largest accepted retry delay, from 250 to 600000 ms. Larger delays fail the read |
| `max_response_bytes` | Integer | No | `8388608` | Maximum wire and decompressed response bytes, from 256 to 16777216 |

HTTP 429 honors `Retry-After` and falls back to the Canny reset timestamp if that header is absent. Rate limits may be shared with other jobs using the same key or egress IP.

## Batch and recovery

Use `BATCH`. One reader processes the selected resources so that per-key pacing and board traversal are deterministic. Checkpoints retain the active resource, response position, and any continuation needed to resume. The source verifies accepted pages before replay, and keeps the credential and row payloads outside its checkpoint. Changing the key, resource selection or page size requires a new scan. The API can change between requests, so a source checkpoint does not provide a workspace snapshot. If a saved page changes before replay, the job fails with a checkpoint diagnostic.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Canny {
    token = "${CANNY_TOKEN}"
    resources = ["boards", "categories", "posts", "votes"]
  }
}
sink {
  Console {}
}
```

<ChangeLog />
