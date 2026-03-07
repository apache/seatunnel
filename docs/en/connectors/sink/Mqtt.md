import ChangeLog from '../changelog/connector-mqtt.md';

# MQTT

> MQTT sink connector

## Description

Used to write data to an MQTT broker. Supports MQTT 3.1.1 protocol via the Eclipse Paho client library.

This connector is suitable for publishing SeaTunnel pipeline data to IoT endpoints and lightweight message brokers. Messages are serialized as JSON or plain text and published to a configurable MQTT topic.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

MQTT is a stateless publish/subscribe protocol without distributed transaction support. The connector provides **at-least-once** delivery semantics by relying on SeaTunnel's source-replay mechanics and MQTT QoS 1.

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
| retry_timeout         | int     | no       | 5000          |
| connection_timeout    | int     | no       | 30            |
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
- `text` — Serialize each row as comma-delimited plain text

### retry_timeout [int]

Maximum time in milliseconds to retry publishing on transient network failures before failing the task. The writer polls the connection state with exponential backoff during this window.

### connection_timeout [int]

The MQTT connection establishment timeout in seconds.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Example

### Simple JSON sink

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  FakeSource {
    row.num = 100
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
  MQTT {
    plugin_input = "sensor_data"
    url = "tcp://broker.example.com:1883"
    topic = "iot/sensors/readings"
    qos = 1
    format = "json"
  }
}
```

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
    retry_timeout = 10000
    connection_timeout = 60
  }
}
```

## Changelog

<ChangeLog />
