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

import ChangeLog from '../changelog/connector-websocket-server.md';

# WebSocketServer

The `WebSocketServer` source listens for inbound RFC 6455 WebSocket connections on a SeaTunnel worker. It emits one record per complete data message in streaming jobs. Install `connector-websocket` and select `WebSocketServer` in the source block.

One worker opens the listener for each job. Configure source parallelism as `1`, publish its worker address and port to your senders, and place the configured key store on each worker eligible to run the source. A restored job can bind on a different worker. Use a stable route to the active listener when worker placement can change.

## Message formats

- `raw` emits `message_type STRING` (`text` or `binary`) and `payload BYTES`. Text frames must contain valid UTF-8 and retain their original bytes.
- `text` decodes UTF-8 and emits a single `value STRING` by default. Configure `schema` and `field_delimiter` to split each message into columns.
- `json` decodes one UTF-8 JSON object per message using the required `schema`. A payload that fails format or schema parsing fails the job.

Clients can send fragmented messages, ping, pong, and close frames. Interleaved control frames are handled while a data message is being assembled. The listener accepts masked client frames and sends unmasked control responses. Frame errors close the connection with the applicable protocol code. One connection preserves message order. Messages from different clients can interleave.

## Configuration

| Option | Type | Default | Meaning |
| --- | --- | --- | --- |
| `bind_host` | String | `127.0.0.1` | Worker address to bind. Set a reachable interface to accept remote clients. |
| `port` | Integer | Required | Listener port, 1–65535. |
| `path` | String | `/` | Exact HTTP upgrade path, up to 4096 characters. Clients send it without a query string. |
| `expected_host` | String | Optional | Exact HTTP Host authority, including a port if configured. A valid Host header is required for every upgrade. |
| `allowed_origins` | List of strings | `[]` | Allowed HTTP(S) origins. An Origin header must match this list. Clients without an Origin header can connect. |
| `token` | String | Optional | Require `Authorization: Bearer <token>`. Set up TLS when sending credentials across a network. |
| `keystore_path` | String | Optional | Key store containing the TLS server certificate and private key. Set this option to serve `wss://`. Clients verify the certificate name using the URL host. |
| `keystore_type` | String | `PKCS12` | Server key store type, `PKCS12` or `JKS`. |
| `password` | String | Required with `keystore_path` | Password for the key store and private key. |
| `format` | String | `raw` | `raw`, `text`, or `json`. |
| `schema` | Config | Required with `json` | Column definitions. Optional with `text`. |
| `field_delimiter` | String | `,` | Delimiter for schema-based text. |
| `max_connections` | Integer | `16` | Concurrent connections, 1–64, including in-progress handshakes. |
| `max_message_bytes` | Integer | `262144` | Complete message bytes, 1–4194304. The product of this limit and `max_connections` must be at most 32 MiB. |
| `max_decoded_units` | Integer | `1048576` | Typed schema/message work units, 1–4194304; slots, entries and visited characters, independent of wire bytes. |
| `max_queue_messages` | Integer | `1024` | Pending message count, 1–10000. Empty messages count toward this limit. |
| `max_queue_bytes` | Integer | `4194304` | Pending payload bytes, from `max_message_bytes` through 32 MiB. |
| `handshake_timeout_ms` | Integer | `5000` | Total TLS and HTTP upgrade deadline, 100–120000 ms. |
| `idle_timeout_ms` | Integer | `20000` | Maximum wait for the next frame on an established connection, 100–600000 ms. |
| `message_timeout_ms` | Integer | `10000` | Total deadline to finish a frame or fragmented message, 100–120000 ms. |

When the queue is full, the listener closes the sending connection with code `1013`. A sender can reconnect and use its own durable buffer and delivery policy. An upgrade, successful frame write, or pong confirms a protocol action only. On savepoint, cancellation, or failure, connections and in-memory messages are discarded. Checkpoints preserve SeaTunnel's source assignment. Senders own replay and downstream deduplication using application identifiers when their delivery contract requires them.

Fragmented messages allow at most 1,024 data fragments, including the initial fragment; control frames do not consume that count. The listener closes an over-limit message with `1009` even below the byte/time caps. TLS deadlines/cancellation abort the independently owned TCP transport, including when a peer stops reading control responses.

For schema-based JSON/TEXT, `max_decoded_units` separately bounds prospective row slots (including missing/null-padded columns), container entries, visited characters and scalar conversion work. The same finite limit applies to schema inspection and retained temporal-formatter paths. Large sparse/repeated values can be rejected even below the wire cap. This is a work/allocation-admission policy, not a whole-JVM heap-byte bound. JSON uses iterative schema/composite conversion and the native scalar semantics, retains the existing Jackson read constraints, and does not retain peer field-name symbols between messages. TEXT admission mirrors the native literal row delimiter, nested control separators, trailing-empty MAP/ARRAY behavior and null padding; it keeps the existing seven-separator boundary. There is no new arbitrary nesting-depth limit. Raw and schema-free text keep their existing wire-only behavior.

## Example

```hocon
env {
  job.mode = "STREAMING"
  parallelism = 1
}
source {
  WebSocketServer {
    bind_host = "0.0.0.0"
    port = 18898
    path = "/ingest"
    expected_host = "ingest.example.com:18898"
    allowed_origins = ["https://app.example.com"]
    token = "${websocket_token}"
    keystore_path = "/etc/seatunnel/ingest.p12"
    password = "${websocket_keystore_password}"
    format = "json"
    schema {
      fields {
        id = bigint
        value = string
      }
    }
  }
}
sink {
  Console {}
}
```

<ChangeLog />
