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

import ChangeLog from '../changelog/connector-http-appfollow.md';

# Appfollow

Appfollow reads account and application data from the AppFollow v2 API. Run it in batch mode with source parallelism 1. The source uses the `X-AppFollow-API-Token` header over verified HTTPS. [AppFollow authorization](https://docs.api.appfollow.io/reference/authorization)

## Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## Resources

| Resource | Endpoint | Payload |
| --- | --- | --- |
| `users` | `GET /api/v2/account/users` | User record from the root response. |
| `app_collections` | `GET /api/v2/account/apps` | Collection record from `apps`. |
| `app_lists` | `GET /api/v2/account/apps/app?apps_id=…` | App record from `apps_app`, enriched with its collection ID and the external ID from `app.ext_id`. |
| `stat_reviews` | `GET /api/v2/reviews/stats?ext_id=…` | Full statistics response object containing `reviews_stat`. |
| `ratings` | `GET /api/v2/meta/ratings?ext_id=…` | Full ratings response object retaining `ratings.list`. |

Selecting an app or metrics resource triggers discovery of its parents. Only selected resources produce rows. The same external app in two collections retains both associations. An application with an absent external ID can be read through `app_lists`. Selecting metrics requires an external ID for each discovered app.

The supported v2 routes return complete responses with no paging parameters. The source fails on an unexpected continuation or invalid shape. AppFollow access and API credit costs vary by method and account. Each discovery, metric request and retry consumes the local request budget. Token usage also accumulates across jobs at AppFollow. [AppFollow API methods and costs](https://docs.api.appfollow.io/reference/methods-cost-and-availability)

## Source options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes | — | AppFollow v2 API token. |
| `resources` | List of strings | No | All five resources | Distinct resources to emit. Required parents are discovered automatically. |
| `api_url` | String | No | `https://api.appfollow.io` | HTTPS origin for api.appfollow.io, with an optional port. |
| `max_entities` | Integer | No | `10000` | Maximum discovered collections and maximum app attachments, each from 1 to 50000. |
| `max_requests` | Integer | No | `1000` | Maximum attempts per reader execution including discovery and retries, from 1 to 100000. |
| `max_records` | Integer | No | `10000` | Maximum records in one response before output, from 1 to 100000. |
| `max_response_bytes` | Integer | No | `16777216` | Separate encoded and decoded response byte limits, from 1024 to 33554432. |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connection timeout, from 1 to 300000. |
| `read_timeout_ms` | Integer | No | `10000` | TLS and read inactivity timeout, from 1 to 300000. |
| `request_timeout_ms` | Integer | No | `30000` | Total request budget including retries, from 1 to 300000. |
| `max_attempts` | Integer | No | `4` | Maximum attempts per request including the first, from 1 to 10. |

## Output and restore

The table contains four string columns.

| Column | Value |
| --- | --- |
| `resource` | Selected resource name. |
| `app_collection_id` | Collection context for collection, app and metrics rows. Null for users. |
| `ext_id` | External app ID for app and metrics rows where available. |
| `data` | Full JSON record with nested fields, nulls and exact numeric values. |

Checkpoints preserve discovered collection and app IDs, the active resource and parent position, and a digest for a partially emitted response. Restore skips completed requests and fetches outstanding responses again. A changed partial response fails validation before the reader resumes at its saved offset. A restored reader starts a new local attempt budget. AppFollow records consumed earlier can change independently.

TLS uses the JVM trust store and checks the configured hostname. Redirects, authentication, permissions, credit, invalid request and required integration errors fail the job. Temporary HTTP 429, 500, 502, 503 and 504 statuses can retry within the attempt and time limits. The token is omitted from source errors and checkpoint state.

## Example

Pass the token as a SeaTunnel job variable.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Appfollow {
    token = "${APPFOLLOW_API_TOKEN}"
    resources = ["users", "app_collections"]
    max_requests = 20
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
