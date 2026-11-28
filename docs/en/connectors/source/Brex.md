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

import ChangeLog from "../changelog/connector-http-brex.md";

# Brex

## Description

The Brex source reads six collections in a bounded batch. It sends HTTPS GET requests to the [Brex API](https://developer.brex.com/) with a dashboard user token. Create a token with read access to the selected resources. The records available depend on the account, token permissions and user role. [User token setup](https://developer.brex.com/guides/authentication) and [roles and permissions](https://developer.brex.com/guides/roles_permissions_scopes).

| Resource | Request path | Recommended read scope | Record key |
| --- | --- | --- | --- |
| `transactions` | `/v2/transactions/card/primary` | `transactions.card.readonly` | `id` |
| `users` | `/v2/users` | `users.readonly` | `id` |
| `departments` | `/v2/departments` | `departments.readonly` | `id` |
| `vendors` | `/v1/vendors` | `vendors.readonly` | `id` |
| `expenses` | `/v1/expenses` with `expense_type[]=CARD` | `expenses.card.readonly` | `id` |
| `budgets` | `/v2/budgets` | `budgets.readonly` | `budget_id` |

A single source subtask reads the selected resources in the table order. Omit `resources` to read all six. The user collection follows the API's default status visibility unless you supply `user_statuses`. Specify `ARCHIVED` explicitly to fetch users in that status if the token can see them. The transaction collection reads settled primary card transactions. The expense collection reads card expenses through the consolidated endpoint.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| token | string | yes | | Brex dashboard user token. |
| resources | list | no | [] | The resource names to read. An empty list selects all six. |
| user_statuses | list | no | [] | Explicit status filters for `users`. Values are `INVITED`, `ACTIVE`, `DISABLED`, `PENDING_ACTIVATION`, `INACTIVE` and `ARCHIVED`. |
| start_date | string | for transactions or expenses | | Earliest RFC 3339 timestamp, including time zone. |
| end_date | string | no | Batch start instant | Inclusive RFC 3339 cutoff frozen before the first request. |
| transactions_resume_date | string | no | | Last processed UTC posting date in `YYYY-MM-DD` for a new batch. |
| expenses_resume_at | string | no | | Last processed purchase timestamp in RFC 3339 for a new batch. |
| tls_trust_certificate | string | no | JVM default trust | Source-local PEM or base64 DER X.509 trust anchor. Hostname verification still applies. |
| page_size | int | no | 100 | Requested items per page from 1 to 1,000. Card expenses use a maximum of 100. |
| max_pages_per_resource | int | no | 10000 | Maximum accepted pages per resource, including empty pages with a continuation. |
| connect_timeout_ms | int | no | 10000 | TCP connection timeout in milliseconds. |
| request_timeout_ms | int | no | 30000 | Deadline for one HTTPS attempt. |
| operation_timeout_ms | int | no | 60000 | Deadline across one read's attempts and retry delays. Must be at least `request_timeout_ms`. |
| request_interval_ms | int | no | 65 | Minimum spacing between attempts in milliseconds. |
| max_retries | int | no | 3 | Additional attempts for retryable reads. |
| max_retry_delay_ms | int | no | 30000 | Maximum permitted retry delay. A longer `Retry-After` fails the read. |
| max_response_bytes | int | no | 8388608 | Maximum bytes before and after decompression. |

The connector follows opaque paging cursors from `next_cursor` even when the current page is empty or short. The current page completes the resource when `next_cursor` is missing or null. The connector fails on invalid or repeated cursors, overfilled pages, malformed JSON, redirects, and authentication or permission errors. Transient transport failures and HTTP 408, 429, 500, 502, 503 and 504 can be retried within the configured limits. [Brex pagination](https://developer.brex.com/guides/pagination) and [rate limits](https://developer.brex.com/guides/rate_limits).

## Time windows and recovery

Transactions send `posted_at_start` and use the UTC date of the cutoff to filter returned `posted_at_date` values. A transaction on that UTC day is eligible even if the cutoff has an earlier time of day. The source reads every page through the final cursor. When you supply `transactions_resume_date` for a new batch, the source replays one UTC calendar day before that date, clipped at `start_date`. Brex can expose settled transactions after a delay. [Brex API changelog](https://developer.brex.com/changelog).

Card expenses use inclusive `purchased_at_start` and `purchased_at_end` with exact timestamp comparisons. When you supply `expenses_resume_at` for a new batch, the lower bound is one calendar month earlier in UTC, clipped at `start_date`. Both overlaps can return previously extracted rows. Use the resource name and record ID to identify duplicates across batches.

A checkpoint keeps the frozen cutoff, resource and page cursor, a page digest and the emitted row position. A restored reader reuses the active page and verifies its digest before emitting the remaining rows. A completed resource stays completed. An expired cursor or changed page can require a new batch. Brex collections can change while a job is reading them, so results reflect the records returned during the scan. Rotating the user token requires a fresh batch because the checkpoint is bound to its original token fingerprint. Supply the last successful transaction date and expense timestamp when resuming this way.

## Output

The source emits four string columns.

| Column | Meaning |
| --- | --- |
| `resource` | Resource name. |
| `id` | Exact provider record key, including `budget_id` for budgets. |
| `cursor` | UTC posting date for transactions, UTC purchase timestamp for expenses, or null for other resources. |
| `record` | Native record as a JSON string, including nested values, nulls and additional fields. |

Money values are kept in the original currency and signed minor units reported by Brex. The source preserves integer and decimal precision.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Brex {
    token = ${BREX_USER_TOKEN}
    resources = ["transactions", "expenses", "budgets"]
    start_date = "2025-01-01T00:00:00Z"
  }
}
```

## Changelog

<ChangeLog />
