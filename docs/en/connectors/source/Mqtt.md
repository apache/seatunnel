import ChangeLog from '../changelog/connector-mqtt.md';

# MQTT

> MQTT source connector

## Description

Used to read messages from an MQTT broker. Supports MQTT 3.1.1 protocol via the Eclipse Paho client library.

This connector subscribes to a configured MQTT topic, deserializes message payloads as JSON or text, and converts them into SeaTunnel rows.

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

:::caution Delivery semantics

The `qos` option controls MQTT broker-client delivery only. It is not integrated with SeaTunnel checkpointing, so this source does not provide end-to-end exactly-once or at-least-once guarantees.

For persistent MQTT sessions, set `clean_session=false` and configure a stable `client_id`. When `clean_session=false`, the source disconnects without unsubscribing during close, so the broker can retain the subscription according to MQTT session semantics.

The source uses MQTT auto-reconnect. If the client remains disconnected longer than `reconnect_timeout`, the source task fails to avoid a silent ingestion stall.

:::

## Options

| name                | type    | required | default value |
|---------------------|---------|----------|---------------|
| url                 | string  | yes      | -             |
| topic               | string  | yes      | -             |
| schema              | config  | yes      | -             |
| username            | string  | no       | -             |
| password            | string  | no       | -             |
| qos                 | int     | no       | 1             |
| format              | string  | no       | json          |
| field_delimiter     | string  | no       | ,             |
| client_id           | string  | no       | -             |
| clean_session       | boolean | no       | true          |
| connection_timeout  | int     | no       | 30            |
| keep_alive_interval | int     | no       | 60            |
| reconnect_timeout   | int     | no       | 120           |
| max_queue_size      | int     | no       | 1000          |
| common-options      |         | no       | -             |

### url [string]

The MQTT broker connection URL. Must include protocol, host, and port.

Example: `tcp://broker.example.com:1883`

### topic [string]

The MQTT topic to subscribe messages from.

Example: `iot/sensors/temperature`

### schema [config]

The schema fields of upstream data. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

### username [string]

The username for MQTT broker authentication. Leave unset for anonymous access.

### password [string]

The password for MQTT broker authentication. Leave unset for anonymous access.

### qos [int]

The MQTT Quality of Service level used when subscribing to the topic.

This setting only controls delivery between the MQTT broker and the MQTT client. It does not provide end-to-end delivery guarantees in SeaTunnel.

Supported values:

- `0` — MQTT QoS 0
- `1` — MQTT QoS 1

### format [string]

The deserialization format for incoming messages. Supported values:

- `json` — Deserialize each message as a JSON object (default)
- `text` — Deserialize each message as delimited plain text (delimiter controlled by `field_delimiter`)

### field_delimiter [string]

The field delimiter used when `format` is set to `text`. Default is `,`.

Examples: `,`, `|`, `\t`

### client_id [string]

The MQTT client id. If omitted while `clean_session=true`, the connector generates a random client id.

This option is required when `clean_session=false`, because persistent MQTT sessions require a stable client id.

### clean_session [boolean]

Whether to use a clean MQTT session. Default is `true`.

- `true` — Broker discards previous session state. Suitable for stateless operation.
- `false` — Broker can retain session state, including subscriptions. Requires a stable `client_id`.

### connection_timeout [int]

The MQTT connection establishment timeout in seconds.

### keep_alive_interval [int]

The MQTT keep alive interval in seconds.

### reconnect_timeout [int]

Maximum seconds to wait for MQTT auto-reconnect before failing the source. If the MQTT client remains disconnected longer than this timeout, `pollNext()` fails the source task instead of silently waiting forever.

### max_queue_size [int]

Maximum number of MQTT messages buffered in memory before deserialization.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Example

### JSON source

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MQTT {
    url = "tcp://broker.example.com:1883"
    topic = "iot/sensors/readings"
    qos = 1
    format = "json"
    schema = {
      fields {
        id = bigint
        name = string
        temperature = double
      }
    }
    plugin_output = "sensor_data"
  }
}

sink {
  Console {
    plugin_input = "sensor_data"
  }
}
```

### Persistent session source

```hocon
source {
  MQTT {
    url = "tcp://broker.example.com:1883"
    topic = "iot/sensors/readings"
    client_id = "seatunnel-mqtt-source"
    clean_session = false
    qos = 1
    format = "json"
    schema = {
      fields {
        id = bigint
        temperature = double
      }
    }
  }
}
```

## Changelog

<ChangeLog />
