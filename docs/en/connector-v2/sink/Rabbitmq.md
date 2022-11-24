# Rabbitmq

> Rabbitmq sink connector

## Description

Used to write data to Rabbitmq.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

##  Options

| name                        | type    | required  | default value |
|-----------------------------|---------|-----------|---------------|
| host                        | string  | yes       | -             |
| port                        | int     | yes       | -             |
| virtual_host                | string  | yes       | -             |
| username                    | string  | yes       | -             |
| password                    | string  | yes       | -             |
| queue_name                  | string  | yes       | -             |
| url                         | string  | no        | -             |
| network_recovery_interval   | int     | no        | -             |
| topology_recovery_enabled   | boolean | no        | -             |
| automatic_recovery_enabled  | boolean | no        | -             |
| connection_timeout          | int     | no        | -             |
| common-options              |         | no        | -             |

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

the queue to write the message to

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

connection TCP establishment timeout in milliseconds; zero for infinite

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Example

simple:

```hocon
sink {
      RabbitMQ {
          host = "rabbitmq-e2e"
          port = 5672
          virtual_host = "/"
          username = "guest"
          password = "guest"
          queue_name = "test1"
      }
}
```

## Changelog

### next version

- Add Rabbitmq Sink Connector
