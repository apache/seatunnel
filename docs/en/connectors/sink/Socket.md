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

Used to send data to a socket server in streaming or batch mode. Each SeaTunnel row is serialized to a
JSON object via `JsonSerializationSchema` and written to the configured TCP port. **The connector does
not append any delimiter at all** — neither a newline, nor any other separator between records. Multiple
records therefore travel as one undelimited, continuous TCP byte stream of concatenated JSON objects
(for example `{"a":1}{"a":2}{"a":3}`). The output is explicitly *not* line-framed JSON, so the peer
must handle framing itself: parse consecutive JSON values with a streaming JSON parser (such as
Jackson's `MappingIterator`) rather than a line-oriented parser. Tools like `nc -l` only echo the raw
concatenated bytes, so they are useful for a quick single-row check but cannot split records on their
own.

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

* Socket Server Console print data. No delimiter is appended, so multiple rows arrive as concatenated JSON objects in the raw byte stream (line breaks shown here only for readability):

```text
{"name":"jared","age":17}{"name":"jared","age":18}...
```

## Changelog

<ChangeLog />
