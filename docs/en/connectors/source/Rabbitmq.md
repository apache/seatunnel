import ChangeLog from '../changelog/connector-rabbitmq.md';

# RabbitMQ

> RabbitMQ source connector

## Description

The RabbitMQ source connector reads messages from one or more RabbitMQ queues and turns
each AMQP message into a SeaTunnel row. It runs in streaming mode and can consume from a
single queue or from several queues at once with the multi-table `tables_configs` option.

The connector acknowledges messages through RabbitMQ's delivery confirms, and when
`use_correlation_id` is enabled it uses the broker-supplied correlation id to deduplicate
redeliveries after acknowledgement failures. Because RabbitMQ does not allow multiple
consumers on a single queue to safely share partitions, the source must run with
`parallelism = 1` to keep exactly-once delivery consistent.

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

## Options

| name                       | type    | required | default value |
| -------------------------- | ------- | -------- | ------------- |
| host                       | string  | yes      | -             |
| port                       | int     | yes      | -             |
| virtual_host               | string  | no       | -             |
| username                   | string  | no       | -             |
| password                   | string  | no       | -             |
| queue_name                 | string  | no       | -             |
| schema                     | config  | no       | -             |
| tables_configs             | array   | no       | -             |
| url                        | string  | no       | -             |
| routing_key                | string  | no       | -             |
| exchange                   | string  | no       | -             |
| network_recovery_interval  | int     | no       | -             |
| topology_recovery_enabled  | boolean | no       | -             |
| AUTOMATIC_RECOVERY_ENABLED | boolean | no       | -             |
| connection_timeout         | int     | no       | -             |
| requested_channel_max      | int     | no       | -             |
| requested_frame_max        | int     | no       | -             |
| requested_heartbeat        | int     | no       | -             |
| prefetch_count             | int     | no       | -             |
| delivery_timeout           | int     | no       | -             |
| use_correlation_id         | boolean | no       | -             |
| common-options             |         | no       | -             |
| durable                    | boolean | no       | true          |
| exclusive                  | boolean | no       | false         |
| auto_delete                | boolean | no       | false         |

### host [string]

the default host to use for connections

### port [int]

the default port to use for connections

### virtual_host [string]

virtual host – the virtual host to use when connecting to the broker

### username [string]

the AMQP user name to use when connecting to the broker

### password [string]

the password to use when connecting to the broker

`username` and `password` should be configured together.

### url [string]

convenience method for setting the fields in an AMQP URI: host, port, username, password and virtual host

### queue_name [string]

the queue to consume messages from. *Note: Required if `tables_configs` is not configured.*

### routing_key [string]

Optional RabbitMQ routing key inherited from the shared RabbitMQ configuration. It is not required for normal queue consumption.

### exchange [string]

Optional RabbitMQ exchange inherited from the shared RabbitMQ configuration. It is not required for normal queue consumption.

### schema [Config]

#### fields [Config]

the schema fields of upstream data. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md). *Note: Required if `tables_configs` is not configured.*

### tables_configs [array]

Used to read from multiple queues simultaneously. Each object in the array must contain `queue_name` and `schema`.

### network_recovery_interval [int]

how long will automatic recovery wait before attempting to reconnect, in ms

### topology_recovery_enabled [boolean]

if true, enables topology recovery

### AUTOMATIC_RECOVERY_ENABLED [boolean]

If true, enables connection recovery.

The option key is currently uppercase in the connector configuration. Use `AUTOMATIC_RECOVERY_ENABLED`, not `automatic_recovery_enabled`.

### connection_timeout [int]

connection tcp establishment timeout in milliseconds; zero for infinite

### requested_channel_max [int]

initially requested maximum channel number; zero for unlimited
**Note:** The value must be between 0 and 65535 (unsigned short in AMQP 0-9-1).

### requested_frame_max [int]

the requested maximum frame size

### requested_heartbeat [int]

Set the requested heartbeat timeout
**Note:** The value must be between 0 and 65535 (unsigned short in AMQP 0-9-1).

### prefetch_count [int]

prefetchCount the max number of messages to receive without acknowledgement

### delivery_timeout [int]

deliveryTimeout maximum wait time, in milliseconds, for the next message delivery

### use_correlation_id [boolean]

Whether the consumed messages provide a unique correlation id that can be used to deduplicate messages when acknowledgments fail.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details

### durable

- true: The queue will survive on server restart.
- false: The queue will be deleted on server restart.

### exclusive

- true: The queue is used only by the current connection and will be deleted when the connection closes.
- false: The queue can be used by multiple connections.

### auto_delete

- true: The queue will be deleted automatically when the last consumer unsubscribes.
- false: The queue will not be automatically deleted.

## Migration Guide & Configuration Rules

If you are upgrading from a previous version that only supported single-table reads, your existing configuration will work without any changes.

**Configuration Priority:**
- You cannot configure both `tables_configs` and the root-level `queue_name` at the same time. They are mutually exclusive. Doing so will result in a configuration validation error.
- Use `tables_configs` for multi-table mode.
- Use root-level `queue_name` and `schema` for single-queue mode.
- In multi-table mode, put each queue's `schema` inside its own `tables_configs` item.
- If you configure `username`, you must also configure `password`, and vice versa.
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
## Changelog

<ChangeLog />
