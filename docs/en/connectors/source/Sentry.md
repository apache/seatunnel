import ChangeLog from '../changelog/connector-sentry.md';

# Sentry

> Sentry project error-events source.

## Description

Reads one project's error events using `GET /api/0/projects/{organization}/{project}/events/`.
Use this source to analyze error events in an external warehouse. The existing Sentry sink and its DSN authentication are unchanged.

## Key Features

- [x] Batch
- [ ] Streaming
- [ ] Exactly-once

Only BATCH mode and parallelism 1 are supported. The source reads an explicit time window.
The engine-injected failure policy must be `FAIL_FAST`; continuing other tables is not supported by this single-project source.
It does not discover projects, read traces or attachments, or create an immutable snapshot.

## Prerequisites

Create a bearer token with `project:read` access to the chosen project. A sink DSN cannot read events.
For a regional or self-hosted deployment, set its HTTPS origin in `api_base_url`. Redirects are rejected; configure the final API origin directly.
TLS certificate verification remains enabled. HTTP is supported only for fixtures with `mock_mode=true` and the literal dummy `token="mock-token"`.

See the [events API](https://docs.sentry.io/api/events/list-a-projects-error-events/) and
[pagination contract](https://docs.sentry.io/api/pagination/).

## Reading and Recovery Contract

Provide absolute RFC3339 `start_time` and `end_time` with an offset, and start strictly before end.
Both are normalized to UTC and sent on every page. The source sends `full=false` and `sample=false`;
`content` contains the returned summary event, not a full stack trace. `page_size` maps to Sentry's `per_page` parameter.

Pagination follows only the `rel="next"` link whose `results="true"`; a next link with `results="false"` completes the read.
Only its cursor is copied into the next request. The configured origin, endpoint and time bounds are never replaced by the server's link.
Missing/malformed pagination, repeated cursors, invalid rows, duplicate JSON object keys, oversized responses and exceeding `max_pages` fail the job instead of silently truncating it.

HTTP 429, 500, 502, 503, 504 and transport failures are retried within `max_retries`.
`Retry-After` seconds and HTTP dates are respected. Delays exceeding 60 seconds fail so the job can be retried later; the connector never retries earlier than the server requests.
Authentication failures and redirects are not retried. Closing the reader aborts in-flight requests and wakes retry waits.
The timeout bounds HTTP I/O and schedules a per-attempt abort. Synchronous operating-system DNS resolution, JSON parsing and retry waits are not covered by a strict total job deadline.

Recovery replays the entire configured window; cursors are not durable snapshot offsets and are not checkpointed.
The reader holds the checkpoint lock during the bounded read, so use manageable windows and suitable checkpoint timeouts.
Rows already written to a nontransactional sink can be duplicated after a failed attempt. Configure downstream deduplication if needed.
Sentry retention, delayed ingestion, event removal and changes while paginating can affect results. No complete-history, gap-free adjacent-window or snapshot-consistency guarantee is made.

## Output Schema

The schema is fixed; custom `schema` is not supported. All fields use STRING to preserve Sentry identifiers and timestamp representations.

| Field | Nullable | Meaning |
| --- | --- | --- |
| event_id | no | `eventID` from the response |
| group_id | yes | `groupID` |
| project_id | yes | `projectID` |
| date_created | yes | `dateCreated`, unchanged |
| title | yes | Event title |
| message | yes | Event message |
| platform | yes | Event platform |
| content | no | Complete returned event object serialized as JSON |

Missing optional fields become null. Unexpected types fail. JSON numeric values retain decimal precision, but `content` is not byte-identical to the HTTP response.
Event content can contain personal data: use access-controlled sinks and avoid logging rows.
The `token` option is masked by SeaTunnel's default parsed-configuration logging rules.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| token | STRING | yes | - | Bearer token with project read access |
| organization | STRING | yes | - | Organization ID or slug |
| project | STRING | yes | - | Project ID or slug |
| start_time | STRING | yes | - | Absolute RFC3339 start |
| end_time | STRING | yes | - | Absolute RFC3339 end |
| api_base_url | STRING | no | https://sentry.io | HTTPS origin without a path, query, fragment or trailing slash |
| page_size | INT | no | 100 | Events requested per page, 1–100 |
| max_pages | INT | no | 10000 | Page safety limit, 1–100000; fails when exceeded |
| max_retries | INT | no | 3 | Additional transient attempts, 0–5 |
| retry_delay_ms | INT | no | 1000 | Minimum retry delay, 1–60000 ms |
| request_timeout_ms | INT | no | 30000 | Per-attempt HTTP connection/socket timeout and abort deadline, 1–120000 ms |
| max_response_bytes | INT | no | 8388608 | Uncompressed body limit, 1024–16777216 bytes |
| mock_mode | BOOLEAN | no | false | Permit an HTTP fixture with dummy token only |

Source [common options](../common-options/source-common-options.md) such as `plugin_output` remain available.
Project names accept letters, digits, underscores and hyphens. `query`, `full`, `sample`, `dsn` and a custom schema are not source options.

## Example

Set the `SENTRY_TOKEN` and `WAREHOUSE_PASSWORD` environment variables before starting the job. The unquoted HOCON substitutions below resolve their values when the configuration is loaded.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  Sentry {
    plugin_output = "events"
    token = ${SENTRY_TOKEN}
    organization = "example"
    project = "backend"
    start_time = "2026-01-01T00:00:00Z"
    end_time = "2026-01-02T00:00:00Z"
  }
}
sink {
  Jdbc {
    plugin_input = "events"
    url = "jdbc:postgresql://warehouse:5432/analytics"
    driver = "org.postgresql.Driver"
    user = "writer"
    password = ${WAREHOUSE_PASSWORD}
    database = "analytics"
    table = "sentry_events"
    generate_sink_sql = true
  }
}
```

Create the destination table with the eight STRING-compatible columns above and install its JDBC driver.
The integration test runs an engine job against a deterministic Sentry API fixture; it does not establish compatibility with every Sentry cloud or self-hosted release.

## Changelog

<ChangeLog />
