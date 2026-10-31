import ChangeLog from '../changelog/connector-rabbitmq.md';

# RabbitMQ

> RabbitMQ source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to read data from RabbitMQ queues.

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

:::tip

The source must be non-parallel (parallelism set to 1) in order to achieve exactly-once. This limitation is mainly due to RabbitMQ’s approach to dispatching messages from a single queue to multiple consumers.

:::

## Connectivity dry-run

Run `bin/seatunnel.sh --config <your-job.conf> --dry-run connect` with your own job configuration.

This source supports connection/authentication and existing-queue metadata checks. It uses the same URI precedence, virtual host, TLS configuration and configured single-table or `tables_configs` schemas as the normal source, without creating a reader.

- Each configured queue is checked with a passive declaration. No queue is created or deleted, no consumer is registered, and no message is read, acknowledged or published.
- Queues must already exist, even when `passive = false`. A normal job may create a missing queue, but this dry-run deliberately cannot. Exclusive queues owned by another connection cannot be checked.
- A successful check does **not** prove consumer/read permissions, queue declaration compatibility, payload format/schema compatibility, or runtime delivery guarantees. Passive declarations may refresh a queue's expiration timer.
- Socket connection, AMQP handshake and channel RPC waits are capped at 10 seconds each; a smaller positive `connection_timeout` is retained. Zero is bounded for this preflight. Queue checks are sequential, so this is not a 10-second limit for the entire command. DNS resolution follows the JVM resolver.
- Automatic/topology recovery is disabled only on the temporary connection. Cleanup aborts that connection with a one-second wait limit, including after failure. Normal source settings and behavior are unchanged.
- Validation errors do not include raw connection strings, broker text or secret-bearing exception causes. Sink connectivity remains unsupported.

## Options

| name                       | type    | required | default value | description                                                                                                                  |
| -------------------------- | ------- | -------- | ------------- |------------------------------------------------------------------------------------------------------------------------------|
| host                       | string  | yes      | -             | The default host to use for connections.                                                                                    |
| port                       | int     | yes      | -             | The default port to use for connections.                                                                                    |
| virtual_host               | string  | no       | -             | Virtual host to use when connecting to the broker.                                                                          |
| username                   | string  | no       | -             | The AMQP user name to use when connecting to the broker.                                                                    |
| password                   | string  | no       | -             | The password to use when connecting to the broker. Must be configured together with `username`.                              |
| queue_name                 | string  | no       | -             | The queue to consume messages from. Required if `tables_configs` is not configured.                                          |
| schema                     | config  | no       | -             | The schema of upstream data. Required if `tables_configs` is not configured. See [Schema Feature](../../introduction/concepts/schema-feature.md). |
| tables_configs             | array   | no       | -             | Multi-queue configuration. Each item must contain `queue_name` and `schema`. Cannot be configured together with root-level `queue_name`. |
| url                        | string  | no       | -             | Convenience method for setting host, port, username, password and virtual host in an AMQP URI.                              |
| routing_key                | string  | no       | -             | Optional RabbitMQ routing key inherited from the shared RabbitMQ configuration. Not required for normal queue consumption. |
| exchange                   | string  | no       | -             | Optional RabbitMQ exchange inherited from the shared RabbitMQ configuration. Not required for normal queue consumption.    |
| network_recovery_interval  | int     | no       | -             | How long automatic recovery waits before attempting to reconnect, in milliseconds.                                          |
| topology_recovery_enabled  | boolean | no       | -             | Whether to enable topology recovery.                                                                                        |
| AUTOMATIC_RECOVERY_ENABLED | boolean | no       | -             | Whether to enable connection recovery. The option key is currently uppercase in the connector configuration.                |
| connection_timeout         | int     | no       | -             | TCP connection establishment timeout, in milliseconds; `0` means infinite.                                                  |
| requested_channel_max      | int     | no       | -             | Initially requested maximum channel number; `0` means unlimited. Must be between 0 and 65535.                                |
| requested_frame_max        | int     | no       | -             | The requested maximum frame size.                                                                                           |
| requested_heartbeat        | int     | no       | -             | The requested heartbeat timeout. Must be between 0 and 65535.                                                                |
| prefetch_count             | int     | no       | -             | The maximum number of messages to receive without acknowledgement.                                                          |
| delivery_timeout           | int     | no       | -             | Maximum wait time, in milliseconds, for the next message delivery.                                                          |
| use_correlation_id         | boolean | no       | -             | Whether the consumed messages provide a unique correlation id that can be used to deduplicate messages when acknowledgments fail. |
| common-options             |         | no       | -             | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md).                  |
| durable                    | boolean | no       | true          | Whether the queue survives a server restart.                                                                                |
| exclusive                  | boolean | no       | false         | Whether the queue is used only by the current connection and is deleted when the connection closes.                        |
| auto_delete                | boolean | no       | false         | Whether the queue is deleted automatically when the last consumer unsubscribes.                                             |
| format | string | no | json | The message payload format: `json` or `protobuf`. |
| protobuf_schema | string | no | - | Protobuf schema used when `format` is `protobuf`. |
| protobuf_message_name | string | no | - | Protobuf message name used when `format` is `protobuf`. |
| uri | string | no | - | Legacy alias for `url`; configure only one. |
| ssl | boolean | no | false | Enable SSL/TLS for host-and-port connections. |
| passive | boolean | no | false | Check that the configured queue exists without declaring it. |

### host [string]

The default host to use for connections.

### port [int]

The default port to use for connections.

### virtual_host [string]

Virtual host – the virtual host to use when connecting to the broker.

### username [string]

The AMQP user name to use when connecting to the broker.

### password [string]

The password to use when connecting to the broker.

`username` and `password` should be configured together.

### url [string]

Convenience method for setting the fields in an AMQP URI: host, port, username, password and virtual host.

### uri [string]

Legacy alias for `url`. Configure only one of `url` and `uri`.

### ssl [boolean]

Enables SSL/TLS for host-and-port configuration. Use `url` with an `amqps://` URI when the URI itself supplies the connection settings.

When `url` uses an `amqps://` URI, the broker certificate is verified against the JVM trust store with hostname verification enabled. Connections that previously relied on the implicit trust-all behavior with self-signed or private-CA certificates must import the broker certificate into the trust store, or they will fail to connect.

### queue_name [string]

The queue to consume messages from. *Note: Required if `tables_configs` is not configured.*

### routing_key [string]

Optional RabbitMQ routing key inherited from the shared RabbitMQ configuration. It is not required for normal queue consumption.

### exchange [string]

Optional RabbitMQ exchange inherited from the shared RabbitMQ configuration. It is not required for normal queue consumption.

### schema [Config]

#### fields [Config]

The schema fields of upstream data. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md). *Note: Required if `tables_configs` is not configured.*

### tables_configs [array]

Used to read from multiple queues simultaneously. Each object in the array must contain `queue_name` and `schema`.

### format [string]

The message payload format. Supported values are `json` and `protobuf`. The default value is `json`.

### protobuf_schema [string]

Effective when `format` is `protobuf`. Defines the Protobuf schema used to deserialize the RabbitMQ message payload.

### protobuf_message_name [string]

Effective when `format` is `protobuf`. Specifies the Protobuf message name to deserialize.

### network_recovery_interval [int]

How long automatic recovery waits before attempting to reconnect, in milliseconds.

### topology_recovery_enabled [boolean]

If `true`, enables topology recovery.

### AUTOMATIC_RECOVERY_ENABLED [boolean]

If `true`, enables connection recovery.

The option key is currently uppercase in the connector configuration. Use `AUTOMATIC_RECOVERY_ENABLED`, not `automatic_recovery_enabled`.

### connection_timeout [int]

TCP connection establishment timeout in milliseconds; `0` means infinite.

### requested_channel_max [int]

Initially requested maximum channel number; `0` means unlimited.
**Note:** The value must be between 0 and 65535 (unsigned short in AMQP 0-9-1).

### requested_frame_max [int]

The requested maximum frame size.

### requested_heartbeat [int]

The requested heartbeat timeout.
**Note:** The value must be between 0 and 65535 (unsigned short in AMQP 0-9-1).

### prefetch_count [int]

The maximum number of messages to receive without acknowledgement.

### delivery_timeout [int]

Maximum wait time, in milliseconds, for the next message delivery.

### use_correlation_id [boolean]

Whether the consumed messages provide a unique correlation id that can be used to deduplicate messages when acknowledgments fail.

### common options

Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md) for details.

### durable

- `true`: The queue will survive on server restart.
- `false`: The queue will be deleted on server restart.

### exclusive

- `true`: The queue is used only by the current connection and will be deleted when the connection closes.
- `false`: The queue can be used by multiple connections.

### auto_delete

- `true`: The queue will be deleted automatically when the last consumer unsubscribes.
- `false`: The queue will not be automatically deleted.

### passive

- false: Declare the queue with the configured durable, exclusive, and auto-delete settings.
- true: Verify that the queue already exists without creating or modifying it. Use this for consumer accounts without queue-declaration permission.

## Migration Guide & Configuration Rules

If you are upgrading from a previous version that only supported single-table reads, your existing configuration will work without any changes.

**Configuration Priority:**
- You cannot configure both `tables_configs` and the root-level `queue_name` at the same time. They are mutually exclusive. Doing so will result in a configuration validation error.
- Use `tables_configs` for multi-table mode.
- Use root-level `queue_name` and `schema` for single-queue mode.
- In multi-table mode, put each queue's `schema` inside its own `tables_configs` item.
- When `format` is `protobuf`, configure both `protobuf_schema` and `protobuf_message_name` at the same level as the queue configuration.
- If you configure `username`, you must also configure `password`, and vice versa.
- Configure only one of `url` and `uri`. `uri` is retained for existing configurations; use `url` in new configurations.
- Set `ssl = true` when connecting to an AMQPS endpoint with `host` and `port` settings.
- `host` and `port` are always required. `virtual_host` is optional unless your RabbitMQ deployment requires a non-default virtual host.

## Example

### Single-table Read Example

```hocon
env {
    parallelism = 1
    job.mode = "STREAMING"
}

source {
    RabbitMQ {
        host = "rabbitmq-e2e"
        port = 5672
        virtual_host = "/"
        username = "guest"
        password = "guest"
        queue_name = "test"
        durable = true
        exclusive = false
        auto_delete = false
        schema = {
            fields {
                id = bigint
                c_map = "map<string, smallint>"
                c_array = "array<tinyint>"
                c_string = string
                c_boolean = boolean
            }
        }
    }
}

sink {
    Console {}
}
```

### Multi-table Read Example

You can use the `tables_configs` option to consume messages from multiple RabbitMQ queues simultaneously within a single job. The connector will automatically assign the correct table identifier to each row based on the queue it originated from, allowing you to route them to different sinks using `plugin_input`.

```hocon
env {
    parallelism = 1
    job.mode = "STREAMING"
}

source {
  RabbitMQ {
    host = "rabbitmq-e2e"
    port = 5672
    virtual_host = "/"
    username = "guest"
    password = "guest"

    # Use tables_configs to read from multiple queues
    tables_configs = [
      {
        queue_name = "users_queue"
        schema = {
          table = "users_table" # Defines the table name for routing
          fields {
            user_id = bigint
            name = string
          }
        }
      },
      {
        queue_name = "orders_queue"
        schema = {
          table = "orders_table" # Defines the table name for routing
          fields {
            order_id = bigint
            amount = double
          }
        }
      }
    ]
  }
}

sink {
  # The first sink will only receive data from users_table
  Console {
    plugin_input = "users_table"
  }

  # The second sink will only receive data from orders_table
  Console {
    plugin_input = "orders_table"
  }
}
```

### Protobuf Read Example

```hocon
source {
    RabbitMQ {
        host = "rabbitmq-e2e"
        port = 5672
        queue_name = "protobuf_queue"
        format = protobuf
        protobuf_message_name = Person
        protobuf_schema = """
            syntax = "proto3";
            message Person {
              int64 id = 1;
              string name = 2;
            }
        """
        schema = {
            fields {
                id = bigint
                name = string
            }
        }
    }
}
```

## FAQ

### Why must parallelism be set to 1 to achieve exactly-once?

RabbitMQ dispatches messages among multiple active consumers on the same queue in a round-robin manner. When multiple parallel readers consume from the same queue, message ordering and deterministic offset/acknowledgement coordination across distributed workers cannot be guaranteed. Therefore, setting parallelism to 1 is required for deterministic exactly-once delivery.

### What message formats are supported by RabbitMQ source?

RabbitMQ source supports JSON by default and Protobuf when `format` is set to `protobuf`. The connector deserializes each RabbitMQ message payload into one SeaTunnel row according to the configured `schema`.

### How does the source handle unacknowledged messages when a failure occurs?

When a SeaTunnel task fails or crashes, the RabbitMQ connection drops, and RabbitMQ automatically requeues any unacknowledged messages. Upon job restoration from a checkpoint, the reader resumes processing without message loss.

## Restoring legacy checkpoints

Checkpoints containing the legacy `RabbitmqSourceState` serialized by SeaTunnel 2.3.13 can be read
by the current RabbitMQ source. That legacy state did not contain assigned queue splits, so restore
starts with an empty assignment map and rediscovers the queues in the current configuration. Keep
the queue configuration and broker data available during upgrade; this compatibility layer does not
reconstruct message offsets or acknowledgements that were absent from the old checkpoint.

Current checkpoints retain assigned splits. Broker messages that were not acknowledged before a
failure can be redelivered; legacy-state compatibility does not add an exactly-once guarantee.

## Changelog

<ChangeLog />
