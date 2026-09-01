import ChangeLog from '../changelog/connector-rabbitmq.md';

# RabbitMQ

> RabbitMQ sink connector

## Description

Used to write data to RabbitMQ queues.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Options

|            name            |  type   | required | default value | description                                                                                                                  |
|----------------------------|---------|----------|---------------|------------------------------------------------------------------------------------------------------------------------------|
| host                       | string  | yes      | -             | The default host to use for connections.                                                                                    |
| port                       | int     | yes      | -             | The default port to use for connections.                                                                                    |
| virtual_host               | string  | yes      | -             | Virtual host to use when connecting to the broker.                                                                          |
| username                   | string  | no       | -             | The AMQP user name to use when connecting to the broker.                                                                    |
| password                   | string  | no       | -             | The password to use when connecting to the broker. Must be configured together with `username`.                              |
| queue_name                 | string  | yes      | -             | The queue to write the message to. If `routing_key` is not configured, the connector publishes to this queue through the default exchange. |
| url                        | string  | no       | -             | Convenience method for setting host, port, username, password and virtual host in an AMQP URI.                              |
| routing_key                | string  | no       | -             | Routing key used to publish messages. Configure together with `exchange` to publish through a specific exchange.              |
| exchange                   | string  | no       | -             | Exchange used when `routing_key` is configured.                                                                             |
| network_recovery_interval  | int     | no       | -             | How long automatic recovery waits before attempting to reconnect, in milliseconds.                                          |
| topology_recovery_enabled  | boolean | no       | -             | Whether to enable topology recovery.                                                                                        |
| AUTOMATIC_RECOVERY_ENABLED | boolean | no       | -             | Whether to enable connection recovery. The option key is currently uppercase in the connector configuration.                |
| connection_timeout         | int     | no       | -             | TCP connection establishment timeout, in milliseconds; `0` means infinite.                                                  |
| rabbitmq.config            | map     | no       | -             | Extra RabbitMQ client parameters. See the [official RabbitMQ documentation](https://www.rabbitmq.com/configure.html).        |
| common-options             |         | no       | -             | Sink plugin common parameters. See [Sink Common Options](../common-options/sink-common-options.md).                         |
| durable                    | boolean | no       | true          | Whether the queue survives a server restart. Used when the connector declares the target queue.                             |
| exclusive                  | boolean | no       | false         | Whether the queue is used only by the current connection and is deleted when the connection closes.                         |
| auto_delete                | boolean | no       | false         | Whether the queue is deleted automatically when the last consumer unsubscribes.                                             |

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

### queue_name [string]

The queue to write the message to. If `routing_key` is not configured, the connector publishes messages to this queue through the default exchange.

### routing_key [string]

The routing key used to publish messages. Configure it together with `exchange` when you want to publish through a specific exchange instead of directly to `queue_name`.

### exchange [string]

The exchange used when `routing_key` is configured.

### network_recovery_interval [int]

How long automatic recovery waits before attempting to reconnect, in milliseconds.

### topology_recovery_enabled [boolean]

If `true`, enables topology recovery.

### AUTOMATIC_RECOVERY_ENABLED [boolean]

If `true`, enables connection recovery.

The option key is currently uppercase in the connector configuration. Use `AUTOMATIC_RECOVERY_ENABLED`, not `automatic_recovery_enabled`.

### connection_timeout [int]

TCP connection establishment timeout in milliseconds; `0` means infinite.

### rabbitmq.config [map]

In addition to the above parameters that must be specified by the RabbitMQ client, the user can also specify multiple non-mandatory parameters for the client, covering [all the parameters specified in the official RabbitMQ document](https://www.rabbitmq.com/configure.html).

### common options

Sink plugin common parameters. See [Sink Common Options](../common-options/sink-common-options.md) for details.

### durable

- `true`: The queue will survive on server restart.
- `false`: The queue will be deleted on server restart.

### exclusive

- `true`: The queue is used only by the current connection and will be deleted when the connection closes.
- `false`: The queue can be used by multiple connections.

### auto_delete

- `true`: The queue will be deleted automatically when the last consumer unsubscribes.
- `false`: The queue will not be automatically deleted.


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
