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

import ChangeLog from '../changelog/connector-slack.md';

# Slack

Read bounded conversation history with Slack's `conversations.history` Web API.

## Support

- Batch source; set source parallelism to **1**.
- Explicit conversation IDs, opaque cursor pagination and checkpoint restore.
- No Events API streaming, channel discovery, thread-reply traversal (`conversations.replies`), files, users or other Slack data streams. History entries returned by this API are emitted as supplied. Workspace retention, token access and Slack API limits determine which messages are available.

Install the existing `connector-slack` plugin. Existing Slack Sink configurations remain valid.
A bot needs membership in each conversation and the applicable `channels:history`, `groups:history`, `im:history` or `mpim:history` scope. User token access follows Slack's access rules. Private history requires appropriate membership. This connector does not expand token permissions.

## Source options

| Option | Type | Required | Default | Meaning |
| --- | --- | --- | --- | --- |
| oauth_token | string | yes | — | Token sent only in the Authorization header; use your secret provider. |
| channel_ids | list of strings | yes | — | 1–10000 distinct alphanumeric conversation IDs. |
| api_root | string | no | https://slack.com/api/ | Trusted API root ending in `/`; no credentials, query or fragment. HTTP is for fixtures only. All redirects fail. |
| page_size | int | no | 15 | 1–999 requested messages; Slack can cap the effective page size. |
| oldest | string | no | — | Exclusive lower Unix timestamp, e.g. `"1700000000.000001"`. |
| latest | string | no | source creation time | Exclusive upper Unix timestamp, frozen once and preserved in checkpoints. |
| request_timeout_ms | int | no | 30000 | Whole HTTP attempt deadline, 1–300000 ms. |
| max_retries | int | no | 3 | 0–10 additional attempts for transport errors, HTTP 429 and 5xx. |
| retry_backoff_ms | int | no | 1000 | Fixed delay, 1–max_retry_delay_ms. |
| max_retry_delay_ms | int | no | 120000 | 1–300000 ms. A larger or malformed Retry-After fails instead of retrying early. |
| max_response_bytes | int | no | 4194304 | 1–67108864 uncompressed bytes per page; exceeding the limit fails. |
| max_pages_per_channel | int | no | 10000 | 1–100000 pages per conversation, including empty pages; exceeding the limit fails instead of truncating. |

Timestamps must be nonnegative decimal strings of at most 40 characters; `oldest` must be less than `latest`. Values are never converted to floating point. Do not configure a custom schema: the source supplies the fixed schema below. Both boundaries are exclusive (`inclusive=false`).

## Output schema

All eight columns are `STRING`, in this order:

| Column | Meaning |
| --- | --- |
| channel_id | Configured conversation ID (non-null). |
| ts | Original Slack timestamp string (non-null), retaining all decimal digits. |
| thread_ts | Thread timestamp, or null when absent. |
| user | User ID, or null when absent. |
| text | Text preserving Unicode, or null when absent. |
| type | Message/event type, or null when absent. |
| subtype | Message subtype, or null when absent. |
| raw_json | Complete message object as JSON (non-null); whitespace is not preserved. |

## Pagination, failure and recovery

Short and empty pages continue when `response_metadata.next_cursor` is nonempty. An empty cursor ends the conversation. `has_more=true` without a usable cursor, malformed responses and repeated cursors fail before emitting that page. Response memory, page count, cursor length (4096 characters) and retry duration are bounded. Slack HTTP-200 `ok:false`, permanent HTTP errors and redirects fail; they are never interpreted as empty history. Error messages expose only allow-listed Slack error codes and numeric channel position, never response bodies or tokens.

A checkpoint atomically records emitted page progress, conversation position, cursor and the frozen upper boundary. HTTP reads and retry waits run outside checkpoint locks on one owned worker, with at most one pending page. Polling yields while HTTP is pending so graceful savepoint stop can reach resource cleanup without waiting for the remote response. Pending pages are not checkpointed until the polling thread emits them. Restore resumes after pages included in the checkpoint; pages emitted after that checkpoint can replay. Use `(channel_id, ts)` as a downstream idempotence key. This is not an exactly-once sink delivery guarantee. A frozen upper boundary does not make mutable Slack history a transactional snapshot; retention/deletions can change results and expired cursors fail explicitly. Do not change conversation order or window settings when restoring the same job.

The CI API-contract E2E uses a deterministic HTTP fixture and real engine jobs; it does not certify a live workspace or token. For deployed Slack, validate your scopes, membership, retention and method limits separately. See the official [history API](https://docs.slack.dev/reference/methods/conversations.history/) and [rate limits](https://docs.slack.dev/apis/web-api/rate-limits/).

## Example

```hocon
env { parallelism = 1, job.mode = "BATCH" }
source {
  Slack {
    oauth_token = ${SLACK_OAUTH_TOKEN}
    channel_ids = ["C0123456789", "C9876543210"]
    oldest = "1700000000.000001"
    latest = "1700086400.000001"
    page_size = 15
  }
}
sink { Console {} }
```

## Changelog

<ChangeLog />
