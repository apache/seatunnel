import ChangeLog from '../changelog/connector-http-amplitude.md';

# Amplitude

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

## Description

Reads raw events through the [Amplitude Export API](https://amplitude.com/docs/apis/analytics/export).
This is a bounded batch source, not an ingestion sink, a continuous subscription or CDC.
It reuses SeaTunnel's existing HTTP and JSON dependencies; no Amplitude SDK is added.

## Key Features

- [x] Batch
- [ ] Stream
- [ ] Exactly-once
- [ ] Column projection
- [ ] Parallelism

Use `parallelism = 1` and `job.mode = BATCH`.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| api_key | String | Yes | - | Amplitude project API key, used as the Basic-auth username. |
| secret_key | String | Yes | - | Project secret key, used as the Basic-auth password. |
| start | String | Yes | - | Inclusive UTC server-upload hour, `YYYYMMDDTHH`. |
| end | String | Yes | - | Inclusive final UTC server-upload hour; fully elapsed at least two hours ago. |
| schema | Config | Yes | - | Nonempty SeaTunnel JSON output schema. |
| region | String | No | `US` | `US` or `EU`; must match project residency. |
| base_url | String | No | Region-specific | Trusted HTTPS origin override; no path except an optional trailing slash. |
| allow_insecure_http | Boolean | No | `false` | HTTP for isolated dummy-credential fixtures only. |
| connect_timeout_ms | Int | No | `30000` | Connection/pool-acquisition timeout, 1..300000 ms. |
| read_timeout_ms | Int | No | `120000` | Socket inactivity timeout, 1..600000 ms; not a total export deadline. |
| max_retries | Int | No | `3` | Additional transport/429/5xx attempts before emission, 0..10. |
| retry_backoff_ms | Int | No | `1000` | Initial exponential delay with jitter, 1..60000 ms. |
| max_retry_backoff_ms | Int | No | `60000` | Maximum wait, 1..600000 ms; at least the initial backoff. |
| max_event_bytes | Int | No | `1048576` | JSONL record limit excluding newline, 1..16777216 bytes. |
| max_archive_bytes | Long | No | `536870912` | Downloaded ZIP limit on local temporary disk. |
| max_uncompressed_bytes | Long | No | `2147483648` | Separate totals for inflated ZIP entries and decoded JSONL bytes. |
| field_mapping | Map | No | - | Schema column to RFC 6901 JSON Pointer. Empty pointer selects the entire event. |

Both archive-size options accept 1..68719476736 bytes (64 GiB). Defaults intentionally bound an
export below the service limit. Reduce the window instead of increasing limits without sufficient
worker disk and processing time.

## Upload-Time Windows and Availability

`start` and `end` are **inclusive hours in UTC**, selected by Amplitude's `server_upload_time`,
not `event_time`. `20260101T00` through `20260101T01` includes two upload hours.
The inclusive window must be at most 365 days. Strict calendar validation rejects invalid dates.

Amplitude documents a two-hour export availability delay after an hour has elapsed. Therefore an
end hour of 01:00 is accepted no earlier than 04:00 UTC. The connector rejects more recent windows
instead of interpreting their 404 responses as complete data. This cutoff is not a guarantee
against delayed availability: HTTP 404 on an eligible window means no export files are currently
available, and finishes with zero rows. Rerun a window when late data is possible.

One request covers the configured window; the connector does not automatically split oversized
exports. The service's export-size limit still applies independently of connector limits. HTTP 400
fails with a suggestion to check the window and service limit; use smaller windows or the service's
bulk-export facilities for larger historical workloads. Several concurrent jobs share project quotas.

## Authentication and Endpoint Trust

Basic authentication sends the project API key and secret key only in the Authorization header.
US uses `https://amplitude.com/api/2/export`; EU uses
`https://analytics.eu.amplitude.com/api/2/export`.

`secret_key` uses SeaTunnel's existing sensitive-option masking. The project API key is not
automatically masked by that rule. Protect configurations using the engine's normal secret-management
and access controls; do not enable HTTP wire/header logging with real credentials. The connector
does not log authorization, raw events or response bodies. Its split state has no credentials or rows.

A custom `base_url` is an administrator-chosen credential destination: use only an origin you trust.
User information, query parameters, fragments and paths are rejected. HTTPS is required unless
`allow_insecure_http = true` is explicitly set for a trusted test fixture with dummy credentials.
Redirects are rejected without forwarding credentials; cookies and implicit client retries are disabled.
The client requests identity HTTP content encoding and rejects other encodings; compression belongs
inside the export ZIP, not an additional HTTP gzip layer.
Responses are limited to 100 HTTP headers and 8192 characters per header line.

## Archive Validation and Resource Use

Exports are ZIP files containing gzip-compressed JSONL files, potentially several per upload hour.
The reader downloads to a private worker temporary file, validates the ZIP central directory and all
entry CRCs/gzip trailers and record-size bounds, then reads the validated archive a second time to
convert and emit rows. It never extracts entry names as filesystem paths. A maximum of 10000 ZIP
entries and 4 MiB of central-directory metadata are accepted; the directory is checked with bounded
reads before constructing the ZIP reader. Each file must contain one gzip member; concatenated members
and trailing data are rejected. The gzip decoder reuses the repository-managed Commons Compress library.
Optional gzip header metadata is limited to 64 KiB before decoder construction. Ambiguous ZIP end
records, inconsistent ZIP64 declarations and multi-disk archives are rejected.
Entry and decoded-byte totals are bounded independently, including actual
streamed bytes rather than trusting ZIP declarations.

Heap use is proportional to the bounded record size and ZIP metadata, not the entire export.
One parsed row may wait in a single-slot handoff while the worker parses the next record.
Local temporary disk must accommodate up to `max_archive_bytes` per active reader. Validation adds
a full decompression pass before emission. Temporary files are deleted on normal success, failure
and cancellation; an abrupt worker/process termination may require normal operating-system
temporary-file cleanup. No cross-process cleanup or durable local archive is promised.

Truncated archives, corrupt gzip/ZIP data, oversize input, invalid JSON or schema conversion errors
fail the job instead of signalling successful completion. Compression and size errors are checked
before emission; a later JSON/schema error may occur after earlier rows have been accepted.

## Schema and Recovery

Each record must be a JSON object with a textual `event_type`. Other columns use SeaTunnel's JSON
format and normal missing-field/null/type-conversion behavior. Nested properties are not implicitly
flattened; use nested ROW/MAP types or explicit JSON pointers. A missing pointer maps to null.

Recovery deliberately replays the **whole configured window**, even after earlier rows were emitted.
There is no persisted row offset or completed-window marker. This is at-least-once replay, not an
immutable snapshot or exactly-once sink delivery; choose downstream deduplication/upsert keys such
as the event `uuid` according to your project's data contract. No stable export-order assumption is made.

HTTP transport errors, 429 and 5xx are retried only while downloading, before emitting rows, up to
`max_retries`. Backoff uses exponential delay with jitter and honors valid Retry-After seconds or
HTTP dates. A delay above `max_retry_backoff_ms` fails instead of retrying earlier than requested.
Other statuses, invalid archives and decoding failures are not silently retried by the connector.
Checkpoint locks cover only row collection and local terminal state, never network/validation waits.
One connector-owned worker downloads, validates and parses the archive into a single-slot handoff.
Each poll collects at most 128 available rows without waiting for the worker, then returns to the
engine so savepoint/stop can close the reader. Rows are emitted on the polling thread, not from that
worker. Close aborts I/O, stops and joins the worker, and removes the temporary file.
Cancellation aborts active HTTP I/O and wakes retry waits.

## Example

```hocon
env {
  parallelism = 1
  job.mode = BATCH
}
source {
  Amplitude {
    api_key = "<AMPLITUDE_PROJECT_API_KEY>"
    secret_key = "<AMPLITUDE_PROJECT_SECRET_KEY>"
    region = US
    start = "20260101T00"
    end = "20260101T01"
    schema {
      fields {
        event_type = string
        uuid = string
        server_upload_time = string
        amount = "decimal(18,2)"
      }
    }
    field_mapping {
      amount = "/event_properties/amount"
    }
  }
}
sink {
  Console {}
}
```

<ChangeLog />
