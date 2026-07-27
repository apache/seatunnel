import ChangeLog from '../changelog/connector-socket.md';

# Socket

> Socket sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Description

Used to send data to a socket server in streaming or batch mode. Each SeaTunnel row is serialized as one
JSON line and pushed to the configured TCP port. There is no separator appended by default, so the
peer must be able to split incoming bytes into lines (for example `nc -l` or any line-oriented
JSON parser).

> For example, if the data from upstream is [`age: 12, name: jared`], the content send to socket server is the following: `{"name":"jared","age":17}`

## Sink Options

|      Name      |  Type   | Required | Default |                                                   Description                                                   |
|----------------|---------|----------|---------|-----------------------------------------------------------------------------------------------------------------|
| host           | String  | Yes      |         | socket server host                                                                                              |
| port           | Integer | Yes      |         | socket server port                                                                                              |
| max_retries    | Integer | No       | 3       | The number of retries to send record failed. Set to `-1` to retry indefinitely, or `0` to fail immediately.      |
| common-options |         | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details |

:::tip

Socket sink is mainly used for local debugging and simple integrations. It reconnects and retries failed writes according to `max_retries`, but it does not provide exactly-once delivery. The TCP client
opens one connection per writer; `host`/`port` are the *server* endpoint that this client connects to.

:::

## Task Example

> This is randomly generated data written to the Socket side

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Socket {
    host = "localhost"
    port = 9999
    max_retries = 3
  }
}
```

* Start a port listening

```shell
nc -l -v 9999
```

* Start a SeaTunnel task

* Socket Server Console print data

```text
{"name":"jared","age":17}
```

## Changelog

<ChangeLog />
