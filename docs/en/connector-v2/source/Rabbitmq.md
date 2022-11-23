# Rabbitmq

> Rabbitmq source connector

## Description

Used to read data from Rabbitmq.

## Key features

- [ ] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

:::tip
The source must be non-parallel (parallelism set to 1) in order to achieve exactly-once. This limitation is mainly due to RabbitMQ’s approach to dispatching messages from a single queue to multiple consumers.

##  Options

| name                        | type    | required | default value |
|-----------------------------|---------|----------|---------------|
| host                        | string  | yes      | -             |
| port                        | int     | yes      | -             |
| virtual_host                | string  | yes      | -             |
| username                    | string  | yes      | -             |
| password                    | string  | yes      | -             |
| queue_name                  | string  | yes      | -             |
| schema                      | config  | yes      | -             |
| url                         | string  | no       | -             |
| routing_key                 | string  | no       | -             |
| exchange                    | string  | no       | -             |
| network_recovery_interval   | int     | no       | -             |
| topology_recovery_enabled   | boolean | no       | -             |
| automatic_recovery_enabled  | boolean | no       | -             |
| connection_timeout          | int     | no       | -             |
| requested_channel_max       | int     | no       | -             |
| requested_frame_max         | int     | no       | -             |
| requested_heartbeat         | int     | no       | -             |
| prefetch_count              | int     | no       | -             |
| delivery_timeout            | long    | no       | -             |
| common-options              |         | no       | -             |

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

the queue to publish the message to

### routing_key [string]

the routing key to publish the message to

### exchange [string]

the exchange to publish the message to

### schema [Config]

#### fields [Config]

the schema fields of upstream data.

### network_recovery_interval [int]

how long will automatic recovery wait before attempting to reconnect, in ms

### topology_recovery [string]

if true, enables topology recovery

### automatic_recovery [string]

if true, enables connection recovery

### connection_timeout [int]

connection tcp establishment timeout in milliseconds; zero for infinite

### requested_channel_max [int]

initially requested maximum channel number; zero for unlimited
**Note: Note the value must be between 0 and 65535 (unsigned short in AMQP 0-9-1).

### requested_frame_max [int]

the requested maximum frame size

### requested_heartbeat [int]

Set the requested heartbeat timeout
**Note: Note the value must be between 0 and 65535 (unsigned short in AMQP 0-9-1).

### prefetch_count [int]

prefetchCount the max number of messages to receive without acknowledgement

### delivery_timeout [long]

deliveryTimeout maximum wait time, in milliseconds, for the next message delivery

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Example

simple:

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

## Changelog

### next version

- Add Rabbitmq source Connector
