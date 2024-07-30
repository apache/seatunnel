# Activemq

> Activemq sink connector

## Description

Used to write data to Activemq.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

|                name                 |  type   | required | default value |
|-------------------------------------|---------|----------|---------------|
| host                                | string  | no       | -             |
| port                                | int     | no       | -             |
| virtual_host                        | string  | no       | -             |
| username                            | string  | no       | -             |
| password                            | string  | no       | -             |
| queue_name                          | string  | yes      | -             |
| uri                                 | string  | yes      | -             |
| check_for_duplicate                 | boolean | no       | -             |
| client_id                           | boolean | no       | -             |
| copy_message_on_send                | boolean | no       | -             |
| disable_timeStamps_by_default       | boolean | no       | -             |
| use_compression                     | boolean | no       | -             |
| always_session_async                | boolean | no       | -             |
| dispatch_async                      | boolean | no       | -             |
| nested_map_and_list_enabled         | boolean | no       | -             |
| warnAboutUnstartedConnectionTimeout | boolean | no       | -             |
| closeTimeout                        | int     | no       | -             |

### host [string]

the default host to use for connections

### port [int]

the default port to use for connections

### username [string]

the AMQP user name to use when connecting to the broker

### password [string]

the password to use when connecting to the broker

### uri [string]

convenience method for setting the fields in an AMQP URI: host, port, username, password and virtual host

### queue_name [string]

the queue to write the message to

### check_for_duplicate [boolean]

will check for duplucate messages

### client_id [string]

client id

### copy_message_on_send [boolean]

if true, enables new JMS Message object as part of the send method

### disable_timeStamps_by_default [boolean]

disables timestamp for slight performance boost

### use_compression [boolean]

Enables the use of compression on the message’s body.

### always_session_async [boolean]

When true a separate thread is used for dispatching messages for each Session in the Connection.

### always_sync_send [boolean]

When true a MessageProducer will always use Sync sends when sending a Message

### close_timeout [boolean]

Sets the timeout, in milliseconds, before a close is considered complete.

### dispatch_async [boolean]

Should the broker dispatch messages asynchronously to the consumer

### nested_map_and_list_enabled [boolean]

Controls whether Structured Message Properties and MapMessages are supported

### warn_about_unstarted_connection_timeout [int]

The timeout, in milliseconds, from the time of connection creation to when a warning is generated

## Example

simple:

```hocon
sink {
      ActiveMQ {
          uri="tcp://localhost:61616"
          username = "admin"
          password = "admin"
          queue_name = "test1"
      }
}
```

## Changelog

### next version

- Add Activemq Source Connector

