import ChangeLog from '../changelog/connector-http-typeform.md';

# Http-Typeform

> Typeform source connector

## Description

Reads the responses of one form from the Typeform Responses API
(`GET /forms/{form_id}/responses`). Built on `connector-http-base`. Batch mode only. Version 1
reads responses only; forms, workspaces, webhooks, OAuth and flattening `answers` into columns are
not supported.

## Key Features

- [x] Batch
- [ ] Stream
- [ ] Exactly-once
- [ ] Column projection
- [ ] Parallelism

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| api_key | String | Yes | - | Typeform personal access token with the `responses:read` scope (raw token, no `Bearer ` prefix). |
| form_id | String | Yes | - | ID of the form to read. Letters, digits, `_` and `-` only. |
| base_url | String | No | `https://api.typeform.com` | Accounts on Typeform's EU data center use `https://api.eu.typeform.com` or `https://api.typeform.eu`. |
| page_size | Int | No | `100` | Responses per page, 1 to 1000 (Typeform's maximum). |
| since | String | No | - | Only responses since this time, inclusive. Unix seconds or ISO 8601 UTC, for example `2026-01-01T00:00:00`. |
| until | String | No | - | Only responses until this time, inclusive. Same format as `since`. |
| response_type | String | No | - | Comma-separated, for example `partial,completed`. Typeform returns completed responses when this is not set. |
| retry | Int | No | `3` | Max retries on HTTP 429 and 5xx. |
| retry_backoff_multiplier_ms | Int | No | `100` | Backoff multiplier in milliseconds. |
| retry_backoff_max_ms | Int | No | `10000` | Maximum backoff time in milliseconds. |

### api_key
Sent only in the `Authorization: Bearer <token>` header, never in query parameters. The connector
never logs headers or the config, and persists no state, so the token does not reach checkpoints.
Whether the engine masks `api_key` when it logs the job config is outside this connector.

### since / until / response_type
Passed to Typeform as is. For completed responses Typeform filters `since` and `until` on
`submitted_at`; for other response types it uses a different timestamp (see Typeform's API
reference).

### retry
Requests are spaced at least 500 ms apart, because Typeform allows 2 requests per second per
account. HTTP 429 and 5xx are retried with a backoff that doubles from
`retry_backoff_multiplier_ms` up to `retry_backoff_max_ms`. `Retry-After` is not honored.

## Output columns

| Column | Type | Notes |
| --- | --- | --- |
| response_id | STRING | Typeform response id |
| landing_id | STRING | |
| token | STRING | Used as the paging cursor |
| landed_at | STRING | ISO 8601 timestamp |
| submitted_at | STRING | ISO 8601 timestamp; null for partial responses |
| answers | STRING | The `answers` array as a JSON string, unchanged. NULL when the response has no answers (empty array or missing). |
| hidden | STRING | The `hidden` fields object as a JSON string, unchanged. `{}` when the form has no hidden fields; NULL when missing. |

Answer shapes differ by question type, so `answers` is kept as raw JSON. Use a JSON transform or
the sink's JSON functions to extract the fields you need.

## Example

```hocon
env {
  parallelism = 1
  job.mode = BATCH
}

source {
  Typeform {
    api_key = "<PERSONAL_ACCESS_TOKEN>"
    form_id = "<FORM_ID>"
    since = "2026-01-01T00:00:00"
    plugin_output = "typeform_responses"
  }
}

sink {
  Console {
    plugin_input = "typeform_responses"
  }
}
```

## Notes

Each run is a bounded snapshot of the form's responses, optionally narrowed by `since` and
`until`. Pages are read newest first: the `token` of the last response on a page is sent as the
`before` cursor, and an empty page ends the read. Responses submitted during the read arrive at the
newest end and do not shift the pages being read. Sort order is not configurable, because the
`before` cursor depends on Typeform's default order.

No incremental state is stored. After a failure, a restarted job re-reads from the first page and
may emit rows that were already sent. Every row includes `response_id`, which a downstream sink can
use to deduplicate or upsert, but the connector provides no delivery guarantee.

Typeform notes that responses from roughly the last 30 minutes may not be returned yet.

The whole form is read in one uninterrupted pass, with a short pause between requests, so on a very
large form checkpoint triggering waits until the read finishes.

<ChangeLog />
