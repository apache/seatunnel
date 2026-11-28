import ChangeLog from '../changelog/connector-http-feishu.md';

# Feishu

{/* Licensed to the Apache Software Foundation (ASF) under one or more
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
    limitations under the License. */}

The Feishu source reads the records of one Feishu or Lark Bitable (Base) table in a bounded batch. Set source parallelism to `1`. Use one source configuration per table. The sink controls whether a full scan appends or replaces data.

## Access

Create a custom Feishu or Lark app and publish it with Bitable read permission. Give the app access to the Base. For a Base with advanced permissions, follow the provider's app authorization instructions and grant the required management access. The provider can otherwise return an empty result. Find the Base token and table ID in the Bitable URL. Use the Bitable token itself for Bases embedded in a wiki.

The source obtains a tenant access token from the app ID and secret and uses it to search the table's records. Configure the Feishu or Lark API origin for your account region. Supply credentials through your secret management mechanism.

## Source options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `app_id` | String | Yes | | Custom app ID. |
| `client_secret` | String | Yes | | Custom app secret. |
| `token` | String | Yes | | Bitable Base token. |
| `table_id` | String | Yes | | Bitable table ID. |
| `api_base_url` | String | No | `https://open.feishu.cn` | HTTPS origin. For Lark, use `https://open.larksuite.com`. Supply the origin without an API path, query, fragment, or embedded credentials. |
| `allow_insecure_http` | Boolean | No | `false` | Allows an HTTP origin for an isolated test provider. |
| `page_size` | Integer | No | `100` | Maximum records requested per page, from 1 to 500. |
| `max_pages` | Integer | No | `10000` | Maximum pages in one scan, from 1 to 10000. The job fails if the table requires more pages. |
| `page_timeout_ms` | Integer | No | `60000` | Deadline for one page including authentication, body reception and retries, from 1000 to 300000 ms. |
| `max_retries` | Integer | No | `3` | Rate limit and server error retries per API request, from 0 to 5. |

The Base token and table ID accept letters, digits, underscores and hyphens. The source rejects redirects. HTTP 429, HTTP 5xx, and provider rate-limit code 99991400 are retried within the page deadline. The source uses the longer valid delay from `Retry-After` and the platform's `x-ogw-ratelimit-reset` header when present. It fails if the delay cannot fit within the page deadline. Each page can replay once after the provider reports an expired tenant token. The response limit is 16 MiB for a page and 64 KiB for token acquisition.

## Output schema

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `record_id` | STRING | No | Bitable record ID. |
| `id` | STRING | Yes | Optional ID returned by the provider. |
| `fields` | STRING | Yes | JSON object containing the table's dynamic field values. |
| `record_json` | STRING | No | Complete provider record as JSON. |

The JSON columns preserve nested objects and arrays, Unicode, explicit JSON nulls and provider timestamp values. A missing or null `fields` value produces SQL null in the projected `fields` column. The complete record retains the distinction between missing properties and explicit JSON null.

## Checkpoint and restore

In a Zeta batch job with checkpointing enabled, the source records the next cursor after it emits a complete page. A savepoint can stop the job while the next request is pending. Restore against the same app, API origin and table. Records emitted after the last completed checkpoint can be replayed. The sink's semantics determine how replays affect downstream data.

Feishu can invalidate cursors or return changed data if the table changes during a scan. Keep the table stable if you need consistent results across pages. This source performs full refreshes. Configure a new full scan when the provider rejects a saved cursor.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Feishu {
    app_id = "${FEISHU_APP_ID}"
    client_secret = "${FEISHU_APP_SECRET}"
    token = "replace-with-base-token"
    table_id = "replace-with-table-id"
  }
}

sink {
  Console {}
}
```

<ChangeLog />
