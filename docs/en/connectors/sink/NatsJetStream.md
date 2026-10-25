import ChangeLog from '../changelog/connector-nats-jetstream.md';

# NatsJetStream

> NATS JetStream sink connector

## Description

NatsJetStream is a **JetStream sink**, not a core NATS publish connector. It writes SeaTunnel rows to a JetStream-enabled NATS Server by using the `io.nats:jnats` client and waiting for the synchronous JetStream publish acknowledgement for each record.

The current implementation targets JetStream-enabled **NATS Server 2.x** with the `jnats` **2.24.0** client used by this connector. The connector documentation only claims compatibility proven by the current implementation and E2E coverage.

The sink does **not** create streams or manage JetStream resources. You must pre-provision a JetStream stream and bind it to the subject, or subject pattern, used by the sink.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

:::caution Delivery semantics

This sink provides **at-least-once** delivery.

A successful synchronous JetStream publish acknowledgement means JetStream accepted that publish request, but it does **not** prove exactly-once delivery across retries, task restarts, failover, or ambiguous acknowledgement loss.

Duplicates can still happen when SeaTunnel retries after a publish succeeded but the acknowledgement was lost, delayed, or surfaced ambiguously to the writer.

Native-mode message IDs are only a **broker-side duplicate-mitigation hint**. JetStream duplicate suppression works only when:

1. the target stream is configured with a duplicate window, and
2. the sink writes stable message IDs.

Even with that setup, the connector must still be treated as **at-least-once**.

:::

## Supported Engines

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## Connector contract

- Scope: sink only; no NATS source support is included.
- Compatibility target: JetStream-enabled NATS Server 2.x with `io.nats:jnats:2.24.0`.
- Stream lifecycle: publish-only. The connector does not create, update, or delete streams, consumers, or subjects.
- Delivery guarantee: writer-only at-least-once.
- Duplicate handling: duplicates are possible on retry, restart, checkpoint recovery, or ambiguous acknowledgement loss.
- JSON mode: one configured subject, JSON payload, no per-record headers or message ID.
- Native mode: maps row fields to `subject`, `id`, `headers`, and `data`; `data` is required and must be `bytes`.
- Row kinds: all SeaTunnel row kinds are published as ordinary sink messages; no CDC-aware update/delete behavior is provided.
- Authentication: use either no credentials, `username` + `password`, or `token`; `token` is mutually exclusive with `username` / `password`.
- Initial limitations: synchronous per-record publish only; batching and connector-managed retry controls are out of scope.
- Non-goals: stream administration, exactly-once, source support, schema evolution handling beyond incoming rows, and formats beyond documented JSON/native mode.

## Broker setup

Before running a SeaTunnel job:

1. Start a JetStream-enabled NATS Server.
2. Create the target stream yourself.
3. Bind the stream to the configured subject or subject pattern.
4. Ensure the subject used by JSON mode or the final subject produced by native mode is covered by that stream binding.

If the stream does not exist, JetStream is disabled, or the subject is not bound to a stream, the sink can fail when the first publish reaches JetStream.

## Options

| name | type | required | default value |
|------|------|----------|---------------|
| url | string | yes | - |
| username | string | no | - |
| password | string | no | - |
| token | string | no | - |
| subject | string | conditional | - |
| format | enum (`json`, `native`) | no | json |
| native_format_fields | map<string,string> | no | `{id:id, subject:subject, headers:headers, data:data}` |
| include_row_kind_header | boolean | no | true |
| common-options | - | no | - |

### Authentication rules

- Configure `username` and `password` together, or configure `token`.
- Do not configure `token` together with `username` / `password`.
- Treat `password` and `token` as sensitive options and do not expose them in shared configs or logs.

### subject [string]

- Required in `json` format.
- Optional in `native` format only when `native_format_fields.subject` is mapped to a nonblank row field.
- In `native` format, it is used as the fallback subject when the mapped subject field is `null`, empty, or blank.

### native_format_fields [map<string,string>]

Used only when `format = "native"`.

Supported mapping keys:

- `data`: required mapping, must point to a `bytes` field
- `subject`: optional mapping, must point to a `string` field when present in the schema
- `id`: optional mapping, must point to a `string` field when present in the schema
- `headers`: optional mapping, must point to a `map<string,string>` field when present in the schema

Unsupported mapping keys are rejected.

When an optional mapping (`subject`, `id`, `headers`) points to a field that does not exist in the input schema, the connector silently skips that mapping. For example, with the default mapping `{id:id, subject:subject, headers:headers, data:data}`, a schema that only contains a `data` field will still be accepted; `id`, `subject`, and `headers` are simply not set on the published message.

### include_row_kind_header [boolean]

Used only when `format = "native"`.

- Default is `true`.
- When enabled, the connector adds JetStream header `x-seatunnel-row-kind` with the SeaTunnel row kind name.
- When disabled, the connector does not add that generated header.
- Mapped `headers` are still written normally.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Data contract

### JSON mode

When `format = "json"`:

- every incoming row is serialized as one JSON payload;
- the payload is written to the fixed sink `subject`;
- no per-record headers or message ID are produced by the connector, and row kind metadata is not added to the JSON payload.

### Native mode

When `format = "native"`, each row is mapped to a JetStream publish request:

- `data` -> message payload, required, `bytes`
- `subject` -> per-record subject, optional, `string`
- `id` -> JetStream message ID, optional, `string`
- `headers` -> JetStream headers, optional, `map<string,string>`
- when `include_row_kind_header = true`, the connector also adds JetStream header `x-seatunnel-row-kind` with the SeaTunnel row kind name

Final subject resolution order:

1. use the mapped native `subject` field when it is nonblank;
2. otherwise use the sink `subject` option;
3. if neither exists, validation fails before the writer starts.

If native `id` is `null`, empty, or blank, the message is published without a JetStream message ID.

If native `headers` is `null`, the connector sends only the generated `x-seatunnel-row-kind` header when `include_row_kind_header = true`.

## Row kind handling

The sink accepts all SeaTunnel row kinds.

It does not interpret `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, or `DELETE` as CDC operations. Each row is published as an ordinary message according to the configured JSON or native format.

In native format, the connector can expose the row kind as JetStream header `x-seatunnel-row-kind` when `include_row_kind_header = true`. JSON format does not include row kind metadata in the payload.

## Examples

### Minimal JSON example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    rows = [
      {
        kind = INSERT
        fields = {
          id = 101
          name = "alice"
          score = 9.5
        }
      }
    ]
    schema = {
      fields {
        id = int
        name = string
        score = double
      }
    }
    plugin_output = "json_fake"
  }
}

sink {
  NatsJetStream {
    plugin_input = "json_fake"
    url = "nats://127.0.0.1:4222"
    subject = "orders.json"
    format = "json"
  }
}
```

Produced payload:

```json
{"id":101,"name":"alice","score":9.5}
```

All rows are published to the fixed subject `orders.json`.

### Native-mode example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    rows = [
      {
        kind = INSERT
        fields = {
          dynamic_subject = "events.native.alpha"
          message_id = "msg-1"
          attributes = {
            tenant = "acme"
            trace = "trace-1"
          }
          payload = [1, 35, -1]
        }
      },
      {
        kind = INSERT
        fields = {
          dynamic_subject = "events.native.beta"
          message_id = "msg-2"
          attributes = {
            tenant = "beta"
            trace = "trace-2"
          }
          payload = [112, 97, 121, 108, 111, 97, 100, 45, 50]
        }
      }
    ]
    schema = {
      fields {
        dynamic_subject = string
        message_id = string
        attributes = "map<string,string>"
        payload = bytes
      }
    }
    plugin_output = "native_fake"
  }
}

sink {
  NatsJetStream {
    plugin_input = "native_fake"
    url = "nats://127.0.0.1:4222"
    subject = "events.native.default"
    format = "native"
    native_format_fields = {
      subject = dynamic_subject
      id = message_id
      headers = attributes
      data = payload
    }
  }
}
```

Native mapping result for the first row:

- subject: `events.native.alpha`
- message ID: `msg-1`
- headers: `tenant=acme`, `trace=trace-1`
- payload bytes: `[1, 35, -1]`

If `dynamic_subject` is blank or `null`, the connector falls back to `subject = "events.native.default"`.

### Native mode with explicit default field mapping

When the input schema already contains `subject`, `id`, `headers`, and `data`, you can use the default mapping values below.

```hocon
native_format_fields = {
  subject = subject
  id = id
  headers = headers
  data = data
}
```

`native_format_fields` still needs to provide the `data` mapping in native mode.

Input schema contract:

```text
subject : string
id      : string
headers : map<string,string>
data    : bytes
```

## Errors and operational notes

- Connection failures fail writer startup.
- Publish failures fail the task with the target subject in the error message.
- Missing stream bindings, wrong subjects, or JetStream API errors can appear on the first publish.
- The writer uses synchronous per-record publish calls, so throughput depends on JetStream acknowledgement latency.

## Unsupported features and limitations

- No exactly-once guarantee.
- No core NATS publish mode without JetStream.
- No stream creation, update, or deletion.
- No connector-managed deduplication window configuration.
- No native payload conversion from non-`bytes` columns.
- No CDC-aware update/delete semantics based on row kind.

## FAQ

### Is this a core NATS publisher?

No. It publishes through the JetStream API and expects JetStream-enabled server-side resources.

### Can the connector create the stream automatically?

No. The stream must be created and bound to the publish subject before the job starts.

### Can message IDs make delivery exactly-once?

No. Message IDs only help JetStream duplicate suppression when the target stream has a duplicate window and producers resend the same stable ID.

## Changelog

<ChangeLog />
