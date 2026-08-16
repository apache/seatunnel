import ChangeLog from '../changelog/connector-activemq.md';

# ActiveMQ

> ActiveMQ sink connector

## Description

Write SeaTunnel rows to an ActiveMQ queue. Each row is serialized as a JSON text message. This is
a sink-only connector; SeaTunnel does not provide an ActiveMQ source connector.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Options

| name                                    | type    | required | default value | description                                                                                                                                                           |
|-----------------------------------------|---------|----------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| uri                                     | string  | yes      | -             | ActiveMQ broker URL, such as `tcp://localhost:61616`.                                                                                                                 |
| queue_name                              | string  | yes      | -             | Queue name to write messages to.                                                                                                                                      |
| username                                | string  | no       | -             | Username used to create the ActiveMQ connection. If this option is set, `password` must also be set.                                                                  |
| password                                | string  | no       | -             | Password used to create the ActiveMQ connection. If this option is set, `username` must also be set.                                                                  |
| client_id                               | string  | no       | -             | JMS client ID used by the connection factory.                                                                                                                         |
| check_for_duplicate                     | boolean | no       | -             | Whether the ActiveMQ client checks duplicate messages.                                                                                                                |
| always_session_async                    | boolean | no       | -             | Whether the ActiveMQ client always uses a separate thread to dispatch messages for each session.                                                                       |
| always_sync_send                        | boolean | no       | -             | Whether the ActiveMQ producer always uses synchronous sends.                                                                                                          |
| close_timeout                           | int     | no       | -             | Timeout in milliseconds before closing the connection is considered failed.                                                                                           |
| dispatch_async                          | boolean | no       | -             | Whether the broker dispatches messages asynchronously.                                                                                                                |
| nested_map_and_list_enabled             | boolean | no       | -             | Whether structured message properties and `MapMessage` entries can contain nested `Map` and `List` objects.                                                           |
| warn_about_unstarted_connection_timeout | int     | no       | -             | Timeout in milliseconds before ActiveMQ warns that a connection was not started correctly. Set a value less than `0` to disable the warning in the ActiveMQ client. |
| consumer_expiry_check_enabled            | boolean | no       | -             | Whether the ActiveMQ client checks message expiration in each `MessageConsumer` before dispatching messages.                                                                                                  |

## Notes

- `uri` is the connection entry point. Put the broker host and port in this value, for example `tcp://activemq-host:61616`.
- `username` and `password` are optional, but they must be configured together when the broker requires authentication.
- The connector writes each SeaTunnel row as one JSON text message to `queue_name`. There is no separate `format` option for this sink.
- Configure the broker address with `uri`. `host` and `port` are not ActiveMQ sink options.
- Use any SeaTunnel source before this sink. The ActiveMQ connector only controls how the final rows are sent to the queue.

## Example

Write fake data to an ActiveMQ queue:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        id = int
        name = string
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "Alice"] }
      { kind = INSERT, fields = [2, "Bob"] }
    ]
  }
}

sink {
  ActiveMQ {
    uri = "tcp://localhost:61616"
    username = "admin"
    password = "admin"
    queue_name = "testQueue"
  }
}
```

In streaming mode, the sink keeps the same broker connection open and writes each row as it
arrives. Username/password can also be embedded in the `uri`, for example
`tcp://admin:admin@localhost:61616`:

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  FakeSource {
    schema = {
      fields {
        id = int
        name = string
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "Alice"] }
    ]
  }
}

sink {
  ActiveMQ {
    uri = "tcp://admin:admin@localhost:61616"
    queue_name = "testQueue"
  }
}
```

## Changelog

<ChangeLog />
