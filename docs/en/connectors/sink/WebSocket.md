import ChangeLog from '../changelog/connector-websocket.md';

# WebSocket Client Sink

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

The WebSocket sink opens an outbound connection to a `ws://` or `wss://` server and sends one message per insert row. Each writer opens its connection on its first valid row. One writer preserves row order. Parallel writers use separate connections. Empty input does not open a connection.

| Format | WebSocket message | Input |
| --- | --- | --- |
| JSON | Text, UTF-8 | The complete input row as one JSON object. |
| TEXT | Text, UTF-8 | The selected STRING column, with no added delimiter. |
| BINARY | Binary | The selected BYTES column, without Base64 encoding. |

TEXT and BINARY require a non-null, nonempty selected value. Update and delete rows fail explicitly. Install the `connector-websocket` plugin through the standard SeaTunnel plugin installer.

## Sink options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| url | String | Yes | - | WebSocket endpoint. Use a nonsecret path and omit user information, query and fragment. |
| format | String | No | `JSON` | `JSON`, `TEXT`, or `BINARY`. |
| field | String | Conditional | - | Input column for TEXT or BINARY. |
| token | String | No | - | Optional bearer token for a `wss://` endpoint. Supply it through your secret mechanism. |
| connect_timeout_ms | Int | No | `10000` | Opening handshake deadline, 1–300000 milliseconds. |
| write_timeout_ms | Int | No | `10000` | Socket write and queue admission deadline, 1–300000 milliseconds. |
| flush_timeout_ms | Int | No | `10000` | Local send queue drain deadline, 1–300000 milliseconds. |
| close_timeout_ms | Int | No | `10000` | Normal closing handshake deadline after draining, 1–300000 milliseconds. |
| max_frame_bytes | Int | No | `1048576` | Maximum uncompressed bytes per outbound message, 1 through `max_queue_bytes`. |
| max_queue_bytes | Int | No | `4194304` | Maximum outstanding payload bytes per writer, 1–16777216. |
| max_inbound_message_bytes | Int | No | `1048576` | Maximum bytes per incoming message, including all continuation fragments; 1–16777216. Oversized input fails before unbounded aggregation. |

The client retains the deployment JVM default proxy selector: plain `ws` uses HTTP proxy forwarding, `wss` uses HTTP CONNECT before TLS, and SOCKS5 routes resolve the origin at the proxy. Existing JVM proxy and bypass settings apply; the connector does not change the global selector. SOCKS5 password challenges use the default JVM authenticator, or `user.name` with an empty password when it supplies no credentials. HTTP proxy authentication is not configured by this sink.

The client uses standard TLS trust and hostname validation and rejects redirects. This sink does not negotiate WebSocket extensions, including permessage-deflate; extension frames fail explicitly. The outgoing queue is bounded by bytes and message count. Incoming application frames are discarded without aggregation; wire frames and cumulative message sizes are bounded by `max_inbound_message_bytes`. A message that exceeds its configured limit fails before enqueue. Connection, write and lifecycle failures fail the writer. Use a trusted receiving service. Inbound data messages do not acknowledge exported rows.

JSON conversion counts each typed value occurrence and input-sized scalar/key work against `max_frame_bytes` before allocating its tree; shared values and overwritten map-key occurrences still consume this budget. Exact serialized bytes are checked independently, including escaping and punctuation. This bounds conversion as well as output, so pathological key collisions or large numeric input text can be rejected even when their normalized final JSON would be smaller. Container conversion and output are iterative without a fixed nesting-depth limit.

## Delivery and recovery

Flush and checkpoint preparation wait for the local send queue to drain. A normal shutdown also waits for the WebSocket closing handshake. Neither local drain nor the connection close confirms application acceptance or durable storage. The sink has no receiver acknowledgement protocol, transactional state, or exactly-once guarantee.

After a connection failure the writer does not reconnect or resend. A failed send can leave a prefix already received by the server. A job retry or source replay after checkpoint restore can send the same rows again. If duplicates matter, make processing idempotent at the receiver.

## Example

This example sends one UTF-8 JSON message per insert row over authenticated TLS.

```hocon
sink {
  WebSocket {
    url = "wss://collector.example.com/events"
    format = "JSON"
    token = "replace-with-bearer-token"
  }
}
```

<ChangeLog />
