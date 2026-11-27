import ChangeLog from '../changelog/connector-splunk.md';

# Splunk

> Splunk sink connector

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## Description

Writes SeaTunnel rows to a Splunk index through the [HTTP Event Collector (HEC)](https://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector).

Each row is serialized into one HEC event envelope: the row itself is written under `event`, and the
Splunk metadata fields (`index`, `source`, `sourcetype`, `host`, `time`) are taken from the sink
options. Events are buffered and POSTed to `/services/collector/event` in batches.

## Key Features

- [ ] [Exactly Once](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [Multiple Table Sink](../../introduction/concepts/connector-v2-features.md)
- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

:::caution Delivery semantics

This sink provides **at-least-once** delivery.

A batch that fails after the collector already indexed it is retried in full, so events can be
duplicated. Splunk's HEC has no server-side deduplication for the `/services/collector/event`
endpoint, and this connector does not send an idempotency key.

:::

:::caution Row kinds

Splunk indexes an append-only event stream, so this sink does not interpret CDC row kinds. Only
`UPDATE_BEFORE` rows are dropped, because indexing a pre-image would store a second, misleading
copy of the record. `INSERT`, `UPDATE_AFTER` and `DELETE` rows are all indexed as ordinary events
and are indistinguishable from one another once in Splunk — a `DELETE` does **not** remove
anything. If you point a CDC-capable source at this sink, expect every change to land as a new
event rather than as a mutation of an existing one.

:::

## Options

| Name                     | Type    | Required | Default | Description                                                                                     |
|--------------------------|---------|----------|---------|-------------------------------------------------------------------------------------------------|
| url                      | string  | Yes      | -       | Splunk HTTP Event Collector address.                                                            |
| token                    | string  | Yes      | -       | HTTP Event Collector token.                                                                     |
| index                    | string  | No       | -       | Target Splunk index.                                                                            |
| source                   | string  | No       | -       | Value written to the Splunk `source` metadata field.                                            |
| sourcetype               | string  | No       | -       | Value written to the Splunk `sourcetype` metadata field.                                        |
| host                     | string  | No       | -       | Static value written to the Splunk `host` metadata field.                                       |
| host_field               | string  | No       | -       | Upstream field whose value populates the Splunk `host` metadata field.                          |
| time_field               | string  | No       | -       | Upstream field whose value populates the Splunk `time` metadata field.                          |
| max_batch_size           | int     | No       | 100     | Maximum number of events sent in one collector request.                                         |
| max_retry_count          | int     | No       | 3       | Maximum number of attempts for one batch request.                                               |
| retry_backoff_ms         | int     | No       | 200     | Base backoff in milliseconds between two attempts of the same batch.                            |
| connect_timeout_ms       | int     | No       | 10000   | Timeout in milliseconds for establishing a connection to the collector.                         |
| socket_timeout_ms        | int     | No       | 60000   | Timeout in milliseconds waiting for collector response data between packets.                    |
| tls_verify_certificate   | boolean | No       | true    | Whether to verify the collector TLS certificate.                                                |
| tls_verify_hostname      | boolean | No       | true    | Whether to verify the collector TLS certificate hostname.                                       |
| multi_table_sink_replica | int     | No       | 1       | Number of sink replicas used by the common multi-table sink routing mechanism.                  |
| common-options           |         | No       | -       | Common sink options.                                                                            |

### url [string]

The HTTP Event Collector address. Both forms are accepted:

- the collector base address, for example `https://splunk-host:8088`, in which case
  `/services/collector/event` is appended;
- the full endpoint address, for example `https://splunk-host:8088/services/collector/event`,
  which is used as-is.

Trailing slashes are stripped. The address must be an absolute `http` or `https` URL including a
host, otherwise the job fails at startup with a message naming the option.

:::caution

Any address already containing `/services/collector` is used **verbatim** — the `/event` suffix is
never appended to it. Some Splunk UI screens show the collector path as
`https://splunk-host:8088/services/collector`, without the suffix; pasting that form sends the JSON
event envelopes to Splunk's **raw** ingestion endpoint, which expects a different payload. Either
configure the base address on its own (`https://splunk-host:8088`) and let the sink append the
path, or give the full endpoint including `/event`.

:::

### token [string]

The HTTP Event Collector token of the target collector, sent as the
`Authorization: Splunk <token>` request header. Treat this value as a secret and prefer passing it
through a job secret or environment variable rather than committing it to a job file.

### index [string]

The Splunk index to write to. When it is not set, the option is omitted from the event envelope and
the collector falls back to the index configured on the HEC token. A token that is not allowed to
write to the configured index makes the collector reject the batch with HTTP 400; the sink treats
this as a permanent failure and fails the task without retrying.

### source [string] / sourcetype [string]

Values written to the Splunk `source` and `sourcetype` event metadata fields. When they are not set,
they are omitted from the envelope and the collector falls back to the values configured on the HEC
token.

### host [string] / host_field [string]

`host` writes a fixed value into the Splunk `host` metadata field for every event. `host_field`
instead names an upstream field whose value is used per event, and takes precedence over `host`.
When `host_field` is set but the row carries a null in that field, the sink falls back to `host`, or
omits the metadata field when `host` is not set either.

The field named by `host_field` also stays in the event body. Drop it upstream with a transform if
you do not want it duplicated.

### time_field [string]

Names an upstream field whose value populates the Splunk `time` metadata field. Supported types:

- `TIMESTAMP` — interpreted as **UTC**, since a SeaTunnel `TIMESTAMP` carries no zone;
- `TIMESTAMP_TZ` — its own offset is used;
- `BIGINT` — interpreted as **epoch milliseconds**.

Any other type fails at startup with a message naming the field and its type. When `time_field` is
not set, or the row carries a null in that field, `time` is omitted and Splunk stamps the event with
its ingest time.

The value is sent as epoch seconds with millisecond precision, which is the representation the
collector expects.

### max_batch_size [int]

The maximum number of events buffered before a collector request is sent. Larger batches reduce
request overhead but increase the number of events replayed when a batch fails.

### max_retry_count [int] / retry_backoff_ms [int]

`max_retry_count` bounds the number of attempts for one batch. Only failures that can clear on
their own are retried: transport errors, HTTP 429 (the collector queue is full) and HTTP 5xx. Every
other response — a bad token, a forbidden index, a malformed payload — fails the task immediately
rather than burning retries on an error that cannot resolve itself.

The backoff grows exponentially from `retry_backoff_ms` and is capped at 20 seconds. The buffer is
cleared only after the collector has accepted the batch, so a failed attempt never silently drops
events.

### tls_verify_certificate [boolean] / tls_verify_hostname [boolean]

Splunk deployments frequently expose the collector behind the self-signed certificate generated at
install time. Set these to `false` to accept it. This disables the protection against
man-in-the-middle attacks and is not recommended outside of test environments; prefer installing a
trusted certificate on the collector.

### common options

Common parameters for Sink plugins. Refer to [Common Sink Options](../common-options/sink-common-options.md) for more details.

## Periodic Flush

The sink flushes its buffer when it reaches `max_batch_size`, on checkpoint, and on close. To also
flush on a timer, set the engine-level `sink.flush.interval` option in the job `env` block:

```hocon
env {
  sink.flush.interval = 3000
}
```

Timer flush is implemented by the Zeta engine only. On Spark and Flink there is no periodic flush,
so tune `max_batch_size` instead.

## Task Example

### Simple

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "splunk_test_table"
    schema = {
      fields {
        id = bigint
        message = string
        hostname = string
        event_time = timestamp
      }
    }
    rows = [
      {fields = [1, "seatunnel event one", "web-01", "2026-08-17T12:30:45"], kind = INSERT},
      {fields = [2, "seatunnel event two", "web-02", "2026-08-17T12:30:46"], kind = INSERT}
    ]
  }
}

sink {
  Splunk {
    plugin_input = "splunk_test_table"
    url = "https://splunk-host:8088"
    token = "00000000-0000-0000-0000-0000000000ff"
    index = "main"
    source = "seatunnel"
    sourcetype = "seatunnel_events"
    host_field = "hostname"
    time_field = "event_time"
    max_batch_size = 100
    max_retry_count = 3
  }
}
```

A row of that job is sent to the collector as:

```json
{"time":1786969845.000,"host":"web-01","source":"seatunnel","sourcetype":"seatunnel_events","index":"main","event":{"id":1,"message":"seatunnel event one","hostname":"web-01","event_time":"2026-08-17T12:30:45"}}
```

### Self-Signed Collector Certificate

```hocon
sink {
  Splunk {
    url = "https://splunk-host:8088"
    token = "00000000-0000-0000-0000-0000000000ff"
    index = "main"
    tls_verify_certificate = false
    tls_verify_hostname = false
  }
}
```

<ChangeLog />
