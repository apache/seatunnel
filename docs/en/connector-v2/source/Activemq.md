import ChangeLog from '../changelog/connector-activemq.md';

# Activemq

> Activemq source connector

## Description

Read data from the ActiveMQ queue.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)

## Options

| name                                |  type   | required | default value |
|-------------------------------------|---------|----------|---------------|
| username                            | string  | no       | -             |
| password                            | string  | no       | -             |
| queue_name                          | string  | yes      | -             |
| uri                                 | string  | yes      | -             |
| schema                              | config  | yes      | -             |
| format                              | string  | no       | json          |
| field.delimiter                     | string  | no       | ,             |
| check_for_duplicate                 | boolean | no       | -             |
| client_id                           | boolean | no       | -             |
| disable_timeStamps_by_default       | boolean | no       | -             |
| dispatch_async                      | boolean | no       | -             |
| warnAboutUnstartedConnectionTimeout | boolean | no       | -             |
| closeTimeout                        | int     | no       | -             |
| use_correlation_id                  | boolean | no       | -             |

### username [string]

the AMQP user name to use when connecting to the broker

### password [string]

the password to use when connecting to the broker

### uri [string]

convenience method for setting the fields in an AMQP URI: host, port, username, password and virtual host

### queue_name [string]

the queue to write the message to

### schema [config]

The structure of the data, including field names and field types.

### format [string]

Data format. The default format is json. Optional text format. The default field separator is ",". 
If you customize the delimiter, add the "field.delimiter" option.

### field.delimiter [string]

Customize the field delimiter for data format

### check_for_duplicate [boolean]

will check for duplucate messages

### client_id [string]

client id

### disable_timeStamps_by_default [boolean]

disables timestamp for slight performance boost

### close_timeout [boolean]

Sets the timeout, in milliseconds, before a close is considered complete.

### dispatch_async [boolean]

Should the broker dispatch messages asynchronously to the consumer

### use_correlation_id [boolean]

Whether the messages received are supplied with a unique id to deduplicate messages (in case of failed acknowledgments).

### warn_about_unstarted_connection_timeout [int]

The timeout, in milliseconds, from the time of connection creation to when a warning is generated

## Example

simple:

```hocon
source {
  ActiveMQ {
    uri="tcp://activemq-host:61616"
    username = "admin"
    password = "admin"
    queue_name = "sourceQueue"
    format = json
    schema = {
      fields {
        id = bigint
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
      }
    }
  }
}
```

## Changelog

<ChangeLog />

