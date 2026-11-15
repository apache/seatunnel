import ChangeLog from '../changelog/connector-websocket.md';

# WebSocket

> WebSocket source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

Used to read data pushed by a WebSocket server over `ws://` or `wss://`. The connector opens a single
connection, optionally sends subscription or authentication messages right after the handshake, and
turns every received frame into SeaTunnel rows using the configured `schema` and `format`.

Unlike the HTTP source, which polls a request/response endpoint, the WebSocket source is driven by the
server: rows are produced whenever the server pushes a frame. Text frames are used as they are, binary
frames are decoded as UTF-8.

The connector uses a single split, so source parallelism is fixed at 1.

## Data Type Mapping

When `schema` is configured, the received message is parsed according to `format` and each field is
converted to the declared SeaTunnel type. For the `json` format the message is parsed as a JSON object
or a JSON array of objects; for the `text` format the message is split by `field_delimiter`.

When `schema` is **not** configured, the whole message is emitted as one row with a single column:

| Column |   SeaTunnel Data type   |
|--------|-------------------------|
| value  | STRING                  |

## Options

|         Name          |  Type   | Required | Default |                                                         Description                                                          |
|-----------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------|
| url                   | String  | Yes      | -       | WebSocket server url, must start with `ws://` or `wss://`.                                                                   |
| headers               | Map     | No       | -       | Headers added to the handshake request, for example `Authorization`.                                                          |
| open_messages         | Array   | No       | -       | Messages sent to the server in order right after the handshake succeeds, usually subscription or authentication payloads. They are re-sent on every successful reconnect. |
| format                | String  | No       | json    | Data format of the received message, only takes effect when `schema` is configured. Supported values are `json` and `text`.   |
| field_delimiter       | String  | No       | ,       | Customize the field delimiter, only takes effect when `format` is `text`.                                                     |
| connect_timeout_ms    | Integer | No       | 12000   | Timeout in milliseconds of establishing the connection.                                                                      |
| ping_interval_ms      | Integer | No       | 0       | Interval in milliseconds of the WebSocket ping frame used to keep the connection alive, `0` disables it.                      |
| enable_reconnect      | Boolean | No       | true    | Whether to reconnect automatically after the connection is broken.                                                           |
| max_reconnect_times   | Integer | No       | 3       | Maximum consecutive reconnect attempts before the task fails.                                                                |
| reconnect_interval_ms | Integer | No       | 3000    | Waiting time in milliseconds before each reconnect attempt.                                                                  |
| queue_capacity        | Integer | No       | 1024    | Capacity of the local queue buffering received messages. The receiving thread blocks once the queue is full, which back-pressures the server. |
| poll_timeout_ms       | Integer | No       | 1000    | Maximum time in milliseconds that a single read waits on the local queue when no message is available.                        |
| max_records           | Long    | No       | -1      | Stop reading after this number of rows has been emitted, only takes effect in batch mode. `-1` means unlimited.               |
| read_timeout_ms       | Integer | No       | -1      | Stop reading after no message has been received for this many milliseconds, counted from the moment the connection is established, only takes effect in batch mode. `-1` means unlimited. |
| common-options        |         | No       | -       | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details. |

:::tip

A WebSocket server keeps pushing data, so with `job.mode = STREAMING` the source never finishes on its
own. With `job.mode = BATCH` you MUST configure `max_records` or `read_timeout_ms`, otherwise the job
would never complete and the connector rejects the configuration.

The connector does not checkpoint a server-side offset, so messages buffered in memory or pushed while
the job is down are lost after a restart. Do not use it when replayable or exactly-once reads are
required.

:::

## How to Create a WebSocket Data Synchronization Jobs

### Streaming Mode

The following example reads a market-data stream and prints it on the local client. The subscription
message is sent as soon as the handshake succeeds:

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  WebSocket {
    url = "wss://example.com/stream"
    headers {
      Authorization = "Bearer <your-token>"
    }
    open_messages = [
      """{"op":"subscribe","args":["trade.BTCUSDT"]}"""
    ]
    ping_interval_ms = 20000
    format = json
    schema = {
      fields {
        symbol = "STRING"
        price = "DOUBLE"
        ts = "BIGINT"
      }
    }
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

### Batch Mode

In batch mode a stop condition is required. The following example collects 100 rows and then finishes,
or gives up earlier if the server pushes nothing for 30 seconds:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  WebSocket {
    url = "ws://localhost:8080/events"
    max_records = 100
    read_timeout_ms = 30000
    schema = {
      fields {
        id = "INT"
        name = "STRING"
      }
    }
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

### Without a Schema

When `schema` is omitted, each frame is emitted verbatim in a single `value` column, which is handy for
inspecting an unknown stream:

```hocon
source {
  WebSocket {
    url = "ws://localhost:8080/events"
    max_records = 10
  }
}
```

## FAQ

### Why does my batch job fail to start with a configuration error?

A WebSocket stream has no natural end, so a batch job needs an explicit stop condition. Configure
`max_records`, `read_timeout_ms`, or both.

### How do I authenticate?

Either put the credentials in `headers` so they are sent with the handshake request, or send them as the
first entry of `open_messages` when the server expects an application-level login frame. Credentials are
never written to the logs.

Some servers instead expect a token in the query string of `url`. That works too: every log line and error
message keeps the parameter names but hides their values, so a url configured as
`wss://stream.example.com/ws?streams=btcusdt@trade&token=secret` is logged as
`wss://stream.example.com/ws?streams=***&token=***`. The connection itself is still made with the `url`
exactly as configured.

A credential placed in the *path* rather than the query string cannot be hidden this way, because the path
identifies the endpoint. Prefer `headers` or `open_messages` for such credentials.

### What happens when the connection drops?

By default the connector reconnects up to `max_reconnect_times` times, waiting `reconnect_interval_ms`
between attempts, and re-sends `open_messages` after each successful reconnect. Once the attempts are
exhausted, the task fails. Set `enable_reconnect = false` to fail on the first disconnection.

### Can I increase the parallelism?

No. The source uses a single split bound to one connection, so its parallelism is fixed at 1. Run one
job per stream instead.

### A single frame contains a JSON array, do I get one row or several?

One row per array element. With `format = json`, a frame holding a JSON array is fanned out into one
row for each element, and `max_records` counts the produced rows, not the received frames.

## Changelog

<ChangeLog />

