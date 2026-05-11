import ChangeLog from '../changelog/connector-edge-socket.md';

# EdgeSocket

> Edge socket source connector for lightweight remote collectors.

## Support Those Engines

> SeaTunnel Zeta

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

`EdgeSocket` is designed for edge collection scenarios where data is produced by lightweight remote
collectors and sent to a central SeaTunnel Zeta cluster over socket.

This source works as a socket ingress server inside Zeta workers: it binds a host/port and accepts
connections from edge collectors.

Internally, `EdgeSocketSourceReader` keeps a local in-memory queue.
The queue capacity is configured by `local_queue_capacity` (default `1024`).
`pollNext()` only pulls records from this local queue.

When a collector pushes a line:

- If queue is full, source replies `RETRY`.
- Otherwise source enqueues the packet and replies `ACK`.

The queue operation logic is encapsulated in the local queue component (`queue` package),
and the socket ingress logic only decodes packet + delegates enqueue.
Compression handling is executed on queue polling path (`pollNext`) according to packet compression type.

## Data Type Mapping

If no `schema` is configured, this source outputs one string field named `value`.
If `schema` is configured, incoming payload is deserialized as JSON into the defined schema.

| SeaTunnel Data type |
|---------------------|
| STRING              |

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| host | String | No | - | Optional externally reachable address for discovery (for example K8s LB/DNS). If omitted, discovery falls back to worker runtime address. Source bind address is always `0.0.0.0` |
| port | Integer | Yes | - | Ingress bind port on Zeta worker |
| local_queue_capacity | Integer | No | 1024 | Local in-memory queue capacity in source reader, must be greater than 0 |
| max_retries | Integer | No | 3 | Maximum retries when reopening ingress socket after bind failure; `-1` means unlimited |
| reconnect_interval_ms | Integer | No | 1000 | Reopen interval in milliseconds |
| accept_timeout_ms | Integer | No | 1000 | Socket accept/read timeout in milliseconds |
| packet_mode | String | No | RAW | Ingress packet mode: `RAW` or `PACKET` |
| aes_secret_key_base64 | String | No | - | Base64 AES key for decrypting `PACKET` mode payload when `encryption=AES_GCM` |
| auth_type | String | No | TOKEN | Authentication type for ingress connection. Current supported value: `TOKEN` |
| auth_token | String | Yes | - | Token value used by `TOKEN` auth_type. Collector must send auth line before any data |
| schema | Config | No | - | Optional schema definition. When configured, payload is parsed by JSON deserialization schema |
| common-options | - | No | - | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md) |

## How to Create an EdgeSocket Synchronization Job

```bash
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  EdgeSocket {
    port = 9999
    auth_type = "TOKEN"
    local_queue_capacity = 1024
    packet_mode = "RAW"
    auth_token = "my-edge-token"
    max_retries = 3
    reconnect_interval_ms = 1000
    accept_timeout_ms = 1000
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

If `host` is omitted, source still binds to `0.0.0.0:<port>`, and discovery falls back to worker runtime address.

## External Packet Protocol

When `packet_mode = "PACKET"`, each line should be one JSON envelope:

```json
{
  "version": 1,
  "payload": "<base64 payload bytes>",
  "compression": "NONE|GZIP|ZLIB|DEFLATE",
  "encryption": "NONE|AES_GCM",
  "iv": "<base64 iv, required for AES_GCM>"
}
```

Processing order is `decrypt + enqueue` on ingress, then `decompress -> utf-8 string` on queue poll.

## Token Authentication

`auth_type` defaults to `TOKEN`. In this mode, `auth_token` is mandatory.

Collector must send the first line:

```text
__AUTH__:<token>
```

Source replies:

- `ACK`: authentication success, collector can send payload lines
- `AUTH_FAILED`: authentication failed, collector should reconnect with corrected token

## Edge Collector Demo

You can use the demo collector script:

```shell
python3 seatunnel-connectors-v2/connector-edge-socket/examples/edge-collector.py --host 127.0.0.1 --port 9999 --token my-edge-token --interval-ms 500
```

Packet mode with gzip/zlib/deflate:

```shell
python3 seatunnel-connectors-v2/connector-edge-socket/examples/edge-collector.py --host 127.0.0.1 --port 9999 --token my-edge-token --packet-mode packet --compression zlib
```

With token auth:

```shell
python3 seatunnel-connectors-v2/connector-edge-socket/examples/edge-collector.py --host 127.0.0.1 --port 9999 --token my-edge-token
```

The demo collector follows the line-based ACK protocol:

- Send auth line first; only start payload sending after `ACK`
- Send one line message
- Wait for source reply
- `ACK` => send next line
- `RETRY` => resend current line later

## Changelog

<ChangeLog />
