import ChangeLog from '../changelog/connector-rabbitmq.md';

# Rabbitmq

> Rabbitmq source connector

## Description

Used to read data from Rabbitmq.

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

:::tip

The source must be non-parallel (parallelism set to 1) in order to achieve exactly-once. This limitation is mainly due to RabbitMQ’s approach to dispatching messages from a single queue to multiple consumers.

:::

## Options

| name                       | type    | required | default value |
| -------------------------- | ------- | -------- | ------------- |
| host                       | string  | yes      | -             |
| port                       | int     | yes      | -             |
| virtual_host               | string  | yes      | -             |
| username                   | string  | yes      | -             |
| password                   | string  | yes      | -             |
| queue_name                 | string  | no       | -             |
| schema                     | config  | no       | -             |
| tables_configs             | array   | no       | -             |
| url                        | string  | no       | -             |
| routing_key                | string  | no       | -             |
| exchange                   | string  | no       | -             |
| network_recovery_interval  | int     | no       | -             |
| topology_recovery_enabled  | boolean | no       | -             |
| automatic_recovery_enabled | boolean | no       | -             |
| connection_timeout         | int     | no       | -             |
| requested_channel_max      | int     | no       | -             |
| requested_frame_max        | int     | no       | -             |
| requested_heartbeat        | int     | no       | -             |
| prefetch_count             | int     | no       | -             |
| delivery_timeout           | long    | no       | -             |
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

### url [string]

convenience method for setting the fields in an AMQP URI: host, port, username, password and virtual host

### queue_name [string]

the queue to consume messages from. *Note: Required if `tables_configs` is not configured.*

### routing_key [string]

the routing key to publish the message to

### exchange [string]

the exchange to publish the message to

### schema [Config]

#### fields [Config]

the schema fields of upstream data. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md). *Note: Required if `tables_configs` is not configured.*

### tables_configs [array]

Used to read from multiple queues simultaneously. Each object in the array must contain a queue_name and a schema.

### network_recovery_interval [int]

how long will automatic recovery wait before attempting to reconnect, in ms

### topology_recovery_enabled [boolean]

if true, enables topology recovery

### automatic_recovery_enabled [boolean]

if true, enables connection recovery

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

### delivery_timeout [long]

deliveryTimeout maximum wait time, in milliseconds, for the next message delivery

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details

### durable

- true: The queue will survive on server restart.
- false: The queue will be deleted on server restart.

### exclusive

- true: The queue is used only by the current connection and will be deleted when the connection closes.
- false: The queue can be used by multiple connections.

### auto-delete

- true: The queue will be deleted automatically when the last consumer unsubscribes.
- false: The queue will not be automatically deleted.

## Migration Guide & Configuration Rules

If you are upgrading from a previous version that only supported single-table reads, your existing configuration will work without any changes.

**Configuration Priority:**
- You cannot configure both `tables_configs` and the root-level `queue_name`/`schema` at the same time. They are mutually exclusive. Doing so will result in a configuration validation error.
- Use `tables_configs` for multi-table mode.
- Use root-level `queue_name` and `schema` for single-table mode.

## Example

### Single-table Read Example

```hocon
source {
    RabbitMQ {
        host = "rabbitmq-e2e"
        port = 5672
        virtual_host = "/"
        username = "guest"
        password = "guest"
        queue_name = "test"
        schema = {
            fields {
                id = bigint
                c_map = "map<string, smallint>"
                c_array = "array<tinyint>"
            }
        }
    }
}
```
### Multi-table Read Example

You can use the `tables_configs` option to consume messages from multiple RabbitMQ queues simultaneously within a single job. The connector will automatically assign the correct table identifier to each row based on the queue it originated from, allowing you to route them to different sinks using `plugin_input`.

```hocon
source {
  RabbitMQ {
    host = "localhost"
    port = 5672
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
  # The first sink will ONLY receive data from the users_queue
  Jdbc {
    plugin_input = "users_table"
    driver = "com.mysql.cj.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/mydb"
    query = "insert into users (user_id, name) values (?, ?)"
  }

  # The second sink will ONLY receive data from the orders_queue
  Jdbc {
    plugin_input = "orders_table"
    driver = "com.mysql.cj.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/mydb"
    query = "insert into orders (order_id, amount) values (?, ?)"
  }
}
```
## Changelog

<ChangeLog />