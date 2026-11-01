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
import ChangeLog from '../changelog/connector-http-mixpanel.md';

# Mixpanel

> Mixpanel raw event export source connector

## Description

Reads one inclusive date window from the [Raw Event Export API](https://docs.mixpanel.com/reference/raw-event-export)
using `GET /api/2.0/export`. Batch mode only, with source parallelism 1. The response is
streamed as JSONL, one JSON object per line, without buffering the complete export or sending
the API's optional `limit` parameter. Exports larger than 100,000 events are not truncated.

This connector reuses the HTTP base module's existing Apache HTTP client dependency directly
for streaming, and the shared JSON deserializer for schema conversion. It does not use the
HTTP base client's buffered string response.

## Key Features

- [x] Batch
- [ ] Stream
- [ ] Exactly-once
- [ ] Column projection
- [ ] Parallelism

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| username | String | Yes | - | Service account username with access to the project. Must not contain a colon or control characters. |
| password | String | Yes | - | Service account secret. Nonempty, no control characters. |
| project_id | Long | Yes | - | Positive Mixpanel project ID. |
| from_date | String | Yes | - | Inclusive start date, strict `yyyy-MM-dd`. |
| to_date | String | Yes | - | Inclusive end date, strict `yyyy-MM-dd`; must not precede `from_date`. |
| region | String | No | `US` | Case-sensitive `US`, `EU` or `IN`. |
| schema | Config | Yes | - | Output schema, using the shared JSON deserializer's supported types. |
| field_mapping | Map | No | - | Schema column to RFC 6901 JSON Pointer into the raw event. Unmapped columns use the same-named top-level field. |
| base_url | String | No | Regional origin | Explicitly trusted HTTPS origin overriding `region`. No userinfo, path other than `/`, query or fragment. |
| allow_insecure_http | Boolean | No | `false` | Allow HTTP for trusted offline fixtures only. Never use real credentials with this setting. |
| connect_timeout_ms | Int | No | `30000` | Connection and connection-pool acquisition timeout; 1 to 300000 ms. |
| read_timeout_ms | Int | No | `120000` | Socket inactivity timeout, not total export duration; 1 to 600000 ms. |
| max_retries | Int | No | `3` | Retries on 429/5xx before event body consumption; 0 to 10. Zero disables retries. |
| retry_backoff_ms | Int | No | `1000` | Initial exponential delay; 1 to 60000 ms and no greater than `max_retry_backoff_ms`. |
| max_retry_backoff_ms | Int | No | `60000` | Retry delay cap; 1 to 600000 ms. Also caps `Retry-After`. |
| max_event_bytes | Int | No | `1048576` | Per-line byte bound, excluding LF but including CR if present; 1 to 16777216. Oversized records fail the task rather than truncate or skip. |

### Authentication And Endpoint Trust

Use a [service account](https://docs.mixpanel.com/reference/service-accounts) with permission
to export from the configured project. The username and secret are Base64-encoded in the
`Authorization: Basic` header; `project_id` is a query parameter. Project tokens and deprecated
project-secret authentication are not supported.

Regional HTTPS origins are `https://data.mixpanel.com` (US),
`https://data-eu.mixpanel.com` (EU), and `https://data-in.mixpanel.com` (IN).
See Mixpanel's [EU](https://docs.mixpanel.com/docs/privacy/eu-residency) and
[India](https://docs.mixpanel.com/docs/privacy/in-residency) residency references.

Only administrators who trust the destination should set `base_url`: that destination receives
the service account credentials. TLS certificate and hostname verification remain enabled.
Redirects are never followed, so a redirect response fails without forwarding credentials.
HTTP is rejected unless `allow_insecure_http = true` is explicitly enabled for a fixture.
The connector does not log credentials, requests, query strings or event/error bodies, and
does not retain transport or JSON exception causes that may contain those values.
Keep HTTP wire/header debug logging disabled when using real credentials.

### Dates And Timestamps

Date strings are passed unchanged to Mixpanel. They are not interpreted using the JVM timezone.
Mixpanel applies UTC for newer projects and the project's timezone for legacy projects.
The current API reference describes UTC for projects created after January 1, 2023, and project
timezone for projects created before January 11, 2023; those published boundaries overlap.
Confirm your project's date interpretation with Mixpanel if it falls in that transition period.

Both endpoints of the date interval are inclusive, so adjacent runs should not repeat a
boundary day unless intentional. Raw `properties.time` values remain Unix seconds; the connector
does not request `time_in_ms` or convert them to local timestamps.

### Schema And Field Mapping

Each record must be one valid JSON object containing a string `event` and object `properties`.
Malformed lines, extra JSON tokens on the same line, invalid shapes, and conversion failures
fail the task. Empty exports succeed; blank lines are not silently skipped. A final valid record
without a trailing newline is accepted. Missing properties mapped to output columns become null.

Use JSON Pointers rather than JSONPath: `/properties/time` selects a nested property,
`/properties/$insert_id` selects the insert ID, and the empty pointer `""` selects the
entire raw event. Escape `/` as `~1` and `~` as `~0` inside a property name.
Mapping keys must name schema columns. STRING columns selecting JSON objects retain their
JSON structure as serialized text, not the original whitespace. You can also declare a nested
ROW or MAP for `properties` using types supported by the shared JSON format.

## Example

Set the placeholders through your normal job configuration process; do not put real secrets
in public configuration files.

```hocon
env {
  parallelism = 1
  job.mode = BATCH
}
source {
  Mixpanel {
    username = "${MIXPANEL_USERNAME}"
    password = "${MIXPANEL_PASSWORD}"
    project_id = 42
    from_date = "2026-01-01"
    to_date = "2026-01-02"
    region = US
    schema {
      fields {
        event = string
        time = bigint
        distinct_id = string
        insert_id = string
        properties = string
        raw_event = string
      }
    }
    field_mapping {
      time = "/properties/time"
      distinct_id = "/properties/distinct_id"
      insert_id = "/properties/$insert_id"
      raw_event = ""
    }
  }
}
sink {
  Console {}
}
```

## Retry, Cancellation And Recovery

Only HTTP 429 and 5xx responses are retried, up to `max_retries` additional requests.
Delays double from `retry_backoff_ms`, bounded by `max_retry_backoff_ms`.
A valid `Retry-After` (seconds or HTTP date) may extend the delay within that same cap.
Other statuses, including 401/403 and redirects, fail immediately. Transport errors are not
retried locally, even before body consumption. No automatic HTTP client retries are enabled.

Once a successful response starts, an interrupted body, malformed event or schema failure
fails the task without retrying that export in place. Cancellation/close aborts the active
request/socket and wakes backoff waits. Failed exports, or cancellation before local completion
intent is committed at valid EOF, do not signal completion. Cancellation after that intent
cannot retract a notification already starting.

The source keeps no resumable event offset. Network reads and retry waits run outside the
checkpoint lock; only row collection and the local completion-intent update use short critical
sections. The completion callback runs outside both checkpoint and lifecycle locks, allowing
cancellation and checkpointing to proceed even while remote completion acknowledgment waits.
Checkpoint state deliberately stores no event progress, so recovery re-reads the complete
configured window, with at-least-once replay:
already emitted events can appear again. This is not exactly-once delivery or an immutable
remote snapshot. Mixpanel also does not deduplicate raw exports. Retain event name,
`properties.distinct_id`, `properties.time` and `properties.$insert_id` to support downstream
deduplication according to Mixpanel's [export guidance](https://docs.mixpanel.com/reference/raw-data-export-api).
Late-arriving events can change a later export of the same dates.

## Limitations

No streaming/incremental cursor, date partitioning, schema discovery, user/group profiles,
cohorts, event filters or sink are included. Choose a manageable date range for the export
duration and Mixpanel rate limits. The per-event size bound limits record buffering, not total
export size; rows retained by downstream operators are outside this reader's memory bound.

<ChangeLog />
