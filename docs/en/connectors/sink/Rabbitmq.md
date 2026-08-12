import ChangeLog from '../changelog/connector-rabbitmq.md';

# RabbitMQ

> RabbitMQ sink connector

## Description

The RabbitMQ sink connector publishes each upstream row as a message to a RabbitMQ
queue, exchange, or routing key. By default messages are sent directly to the named
`queue_name` via the default exchange; when `routing_key` (and optionally `exchange`) is
configured, the connector routes the message through the specified exchange instead.

The sink declares the target queue if it does not already exist, applying the
`durable`, `exclusive`, and `auto_delete` arguments, and reconnects on broker failures
using the standard AMQP recovery options (`network_recovery_interval`,
`topology_recovery_enabled`, `AUTOMATIC_RECOVERY_ENABLED`).

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Options

|            name            |  type   | required | default value |
|----------------------------|---------|----------|---------------|
| host                       | string  | yes      | -             |
| port                       | int     | yes      | -             |
| virtual_host               | string  | yes      | -             |
| username                   | string  | no       | -             |
| password                   | string  | no       | -             |
| queue_name                 | string  | yes      | -             |
| url                        | string  | no       | -             |
| routing_key                | string  | no       | -             |
| exchange                   | string  | no       | -             |
| network_recovery_interval  | int     | no       | -             |
| topology_recovery_enabled  | boolean | no       | -             |
| AUTOMATIC_RECOVERY_ENABLED | boolean | no       | -             |
| connection_timeout         | int     | no       | -             |
| rabbitmq.config            | map     | no       | -             |
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

the queue to write the message to. If `routing_key` is not configured, the connector publishes messages to this queue through the default exchange.

### routing_key [string]

The routing key used to publish messages. Configure it together with `exchange` when you want to publish through a specific exchange instead of directly to `queue_name`.

### exchange [string]

The exchange used when `routing_key` is configured.

### network_recovery_interval [int]

how long will automatic recovery wait before attempting to reconnect, in ms

### topology_recovery_enabled [boolean]

if true, enables topology recovery

### AUTOMATIC_RECOVERY_ENABLED [boolean]

If true, enables connection recovery.

The option key is currently uppercase in the connector configuration. Use `AUTOMATIC_RECOVERY_ENABLED`, not `automatic_recovery_enabled`.

### connection_timeout [int]

connection TCP establishment timeout in milliseconds; zero for infinite

### rabbitmq.config [map]

In addition to the above parameters that must be specified by the RabbitMQ client, the user can also specify multiple non-mandatory parameters for the client, covering [all the parameters specified in the official RabbitMQ document](https://www.rabbitmq.com/configure.html).

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details

### durable

- true: The queue will survive on server restart.
- false: The queue will be deleted on server restart.

### exclusive

- true: The queue is used only by the current connection and will be deleted when the connection closes.
- false: The queue can be used by multiple connections.

### auto_delete

- true: The queue will be deleted automatically when the last consumer unsubscribes.
- false: The queue will not be automatically deleted.


## Configuration Notes

- If you configure `username`, you must also configure `password`, and vice versa.
- `host`, `port`, `virtual_host`, and `queue_name` are required connector options. `url` can additionally provide the AMQP URI used by the RabbitMQ client.
- `durable`, `exclusive`, and `auto_delete` are used when the connector declares the target queue.

## Example

### Write Messages to a Queue

```hocon
env {
    parallelism = 1
    job.mode = "STREAMING"
}

source {
    FakeSource {
        row.num = 10
        schema = {
            fields {
                id = bigint
                c_string = string
            }
        }
    }
}

sink {
      RabbitMQ {
          host = "rabbitmq-e2e"
          port = 5672
          virtual_host = "/"
          username = "guest"
          password = "guest"
          queue_name = "test1"
          rabbitmq.config = {
            requested-heartbeat = 10
            connection-timeout = 10
          }
      }
}
```

### Declare Queue Options

Queue with `durable`, `exclusive`, and `auto_delete`:

```hocon
env {
    parallelism = 1
    job.mode = "STREAMING"
}

source {
    FakeSource {
        row.num = 10
        schema = {
            fields {
                id = bigint
                c_string = string
            }
        }
    }
}

sink {
      RabbitMQ {
          host = "rabbitmq-e2e"
          port = 5672
          virtual_host = "/"
          username = "guest"
          password = "guest"
          queue_name = "test1"
          durable = true
          exclusive = false
          auto_delete = false
          rabbitmq.config = {
            requested-heartbeat = 10
            connection-timeout = 10
          }
      }
}
```

## Changelog

<ChangeLog />
