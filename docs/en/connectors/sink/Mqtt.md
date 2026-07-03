import ChangeLog from '../changelog/connector-mqtt.md';

# MQTT

> MQTT sink connector

## Description

Used to write data to an MQTT broker. Supports MQTT 3.1.1 protocol via the Eclipse Paho client library.

This connector is suitable for publishing SeaTunnel pipeline data to IoT endpoints and lightweight message brokers. Messages are serialized as JSON or plain text and published to a configurable MQTT topic.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

**Delivery Semantics Notice**:
This connector provides **at-most-once** delivery when QoS=0, and **best-effort at-least-once** when QoS=1.
Due to `clean_session=true` (the default, required for stateless operation), unacknowledged messages may be lost during
client disconnections. Setting `clean_session=false` lets the broker keep session state while the writer is running, but
the sink currently generates a unique client id for each writer and does not expose a `client_id` option. For recovery
from job restarts, rely on upstream replay and MQTT QoS rather than a stable sink client id.

## Supported Engines

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## Options

|       name            |  type   | required | default value |
|-----------------------|---------|----------|---------------|
| url                   | string  | yes      | -             |
| topic                 | string  | yes      | -             |
| username              | string  | no       | -             |
| password              | string  | no       | -             |
| qos                   | int     | no       | 1             |
| format                | string  | no       | json          |
| field_delimiter       | string  | no       | ,             |
| batch_size            | int     | no       | 1             |
| retry_timeout         | int     | no       | 5000          |
| connection_timeout    | int     | no       | 30            |
| clean_session         | boolean | no       | true          |
| common-options        |         | no       | -             |

### url [string]

The MQTT broker connection URL. Must include protocol, host, and port.

Example: `tcp://broker.example.com:1883`

### topic [string]

The MQTT topic to publish messages to.

Example: `iot/sensors/temperature`

### username [string]

The username for MQTT broker authentication. Leave unset for anonymous access.

### password [string]

The password for MQTT broker authentication. Leave unset for anonymous access.

### qos [int]

The MQTT Quality of Service level for published messages.

- `0` — At most once (fire and forget)
- `1` — At least once (acknowledged delivery, default)

### format [string]

The serialization format for outgoing messages. Supported values:

- `json` — Serialize each row as a JSON object (default)
- `text` — Serialize each row as delimited plain text (delimiter controlled by `field_delimiter`)

### field_delimiter [string]

The field delimiter used when `format` is set to `text`. Default is `,`.

Examples: `,`, `|`, `\t`

### batch_size [int]

Number of messages to buffer before sending to the broker. Default is `1` (send each message immediately).

Higher values improve throughput by reducing per-message overhead. Buffered messages are automatically flushed at each checkpoint and when the writer closes.

### retry_timeout [int]

Maximum time in milliseconds to retry publishing on transient network failures before failing the task. The writer polls the connection state with exponential backoff during this window.

### connection_timeout [int]

The MQTT connection establishment timeout in seconds.

### clean_session [boolean]

Whether to use a clean MQTT session. Default is `true`.

- `true` — Broker discards any previous session state. Suitable for stateless operation (recommended for most use cases).
- `false` — Broker retains session state for the generated writer client id while the writer is running. This can help with transient disconnects, but it may cause broker-side state accumulation and does not provide a stable client id across job restarts.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Performance Considerations

The MQTT Sink sends messages synchronously to guarantee delivery ordering. Typical throughput:

- QoS 0: ~10,000 messages/sec (local network)
- QoS 1: ~5,000 messages/sec (requires broker ACK)

To improve throughput:

- Increase `batch_size` to reduce per-message overhead (e.g., `batch_size = 100`)
- Reduce `qos` to `0` if at-most-once delivery is acceptable
- Increase SeaTunnel parallelism to distribute load across multiple MQTT clients
- For very high throughput requirements, consider using the Kafka Sink instead

## Example

### Write JSON messages to MQTT

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  job.name = "SeaTunnel_MQTT_Sink"
}

source {
  FakeSource {
    row.num = 16
    schema = {
      fields {
        id = bigint
        name = string
        age = int
      }
    }
    plugin_output = "fake"
  }
}

sink {
  MQTT {
    plugin_input = "fake"
    url = "tcp://mqtt-broker:1883"
    topic = "test/seatunnel/sink"
    qos = 1
    format = "json"
  }
}
```

This job writes 16 rows to the `test/seatunnel/sink` topic. Each row is serialized as one JSON
message because `format` is set to `json`.

### Authenticated broker with text format

```hocon
sink {
  MQTT {
    url = "tcp://secure-broker.example.com:1883"
    topic = "data/pipeline/output"
    username = "seatunnel_user"
    password = "secret"
    qos = 1
    format = "text"
    field_delimiter = "|"
    retry_timeout = 10000
    connection_timeout = 60
  }
}
```

When `format = "text"`, each row is serialized as a delimited text line. Use `field_delimiter` to
match the delimiter expected by downstream consumers.

## Changelog

<ChangeLog />
