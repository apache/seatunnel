import ChangeLog from '../changelog/connector-http-intercom.md';

# Http-Intercom

> Intercom source connector

## Description

Reads contacts from the Intercom REST API (`GET /contacts`). Built on
`connector-http-base`. Batch mode only. Version 1 reads contacts only; conversations, tickets,
companies, search, OAuth and custom attributes are not supported.

## Key Features

- [x] Batch
- [ ] Stream
- [ ] Exactly-once
- [ ] Column projection
- [ ] Parallelism

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| api_key | String | Yes | - | Intercom access token (raw token, no `Bearer ` prefix). |
| base_url | String | No | `https://api.intercom.io` | Use `https://api.eu.intercom.io` or `https://api.au.intercom.io` for EU or AU hosted workspaces. |
| api_version | String | No | `2.17` | Sent as the `Intercom-Version` header, so the response shape does not change with the workspace default. |
| per_page | Int | No | `150` | Contacts per page, 1 to 150 (Intercom's maximum). |
| retry | Int | No | `3` | Max retries on HTTP 429 and 5xx. |
| retry_backoff_multiplier_ms | Int | No | `100` | Backoff multiplier in milliseconds. |
| retry_backoff_max_ms | Int | No | `10000` | Maximum backoff time in milliseconds. |

### api_key
Sent only in the `Authorization: Bearer <token>` header. The connector never logs headers or
the config, and persists no state, so the token does not reach checkpoints. Whether the engine
masks `api_key` when it logs the job config is outside this connector.

### retry
Max retries on HTTP 429 and 5xx (and, in the base client, IOExceptions). Backoff doubles from
`retry_backoff_multiplier_ms` up to `retry_backoff_max_ms`. `Retry-After` is not honored.
Intercom's default limit is 10,000 calls per minute per app (25,000 per workspace), enforced in
10-second windows.

## Output columns

| Column | Type | Notes |
| --- | --- | --- |
| id | STRING | Intercom contact id |
| role | STRING | `user` or `lead` |
| email | STRING | |
| name | STRING | Can be null |
| phone | STRING | Can be null |
| external_id | STRING | Can be null |
| created_at | BIGINT | Unix timestamp in seconds |
| updated_at | BIGINT | Unix timestamp in seconds |

## Example

```hocon
env {
  execution.parallelism = 1
  job.mode = BATCH
}

source {
  Intercom {
    api_key = "<ACCESS_TOKEN>"
    plugin_output = "intercom_contacts"
  }
}

sink {
  Console {
    plugin_input = "intercom_contacts"
  }
}
```

## Notes

Each run is a bounded snapshot of all contacts, read from the first page by following the
`pages.next.starting_after` cursor. The list endpoint has no "updated since" filter, so there is
no incremental mode. After a failure, a restarted job re-reads from the beginning and may emit rows
that were already sent; contacts created or merged during a run can also appear or disappear
between pages. Every row includes the contact `id`, which a downstream sink can use to deduplicate
or upsert, but the connector provides no delivery guarantee.

<ChangeLog />
