import ChangeLog from '../changelog/connector-opcua.md';

# OpcUa

{/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */}

Read live OPC UA values and address-space metadata over OPC TCP. The source uses one split per table and supports Java 11. It requires anonymous user identity. Secured channels use a separate client application certificate.

## Read modes

- `POLLING` reads current values in batches and waits `interval_ms` between reads. It may skip intermediate changes.
- `SUBSCRIBE` installs monitored items. The server can revise the requested publishing or sampling interval.
- `BROWSE_NODES` emits each reachable local Object or Variable once, with its first discovered parent. It excludes the root and remote-server references.

For value modes, select `MANUAL` with `nodes`, `FILE` with an absolute JSON file path accessible to the job submission and engine processes, or `BROWSE` to select Variables below `browse_root`. A file contains an alias-to-NodeId object such as `{"temperature":"nsu=urn:factory:line1;s=temperature"}`. Browse-based selection refreshes periodically and fails if no variables are found. Browse follows continuation points. It fails when the configured or internal limits are exceeded.

NodeIds support `i=` (unsigned numeric), `s=` (string), `g=` (UUID), and `b=` (Base64 bytes), optionally with an `ns=<index>;` or `nsu=<URI>;` prefix. Prefer `nsu=` for server-specific namespaces. For configured indices, the source stores the resolved namespace URI for reconnection and checkpoint recovery.

## Security and recovery

The source selects a server-advertised endpoint with the configured host, port, path, security policy and message mode. Secured endpoints also support discovery on the same host and port at the endpoint path plus `/discovery`. Secured endpoints require the trusted server certificate's SHA-256 fingerprint, expected server application URI and a worker-local client PKCS12 file. The source verifies the certificate's hostname, URI and validity period. Default `SignAndEncrypt` protects confidentiality. None requires `security_policy = "None"` and `allow_insecure = true`. Legacy policies require `allow_legacy_security = true`.

Use a streaming job for continuous values, or set a positive `max_records` to allow a finite value run in BATCH mode. Checkpoints store bound namespace identities, progress toward a finite record limit and completion state. Pending notifications and server history are outside the checkpoint. Reconnection can produce duplicate values or gaps. The source emits `GAP` for detected interruptions, lost notifications, failed transfers, subscription status/watchdog issues, or changes in the monitored node set. Client notification overflow fails the job. Pending I/O is cancelled when the reader stops.

## Output

| Field | Type | Meaning |
| --- | --- | --- |
| kind | STRING | VALUE, NODE or GAP |
| node_id | STRING | Canonical NodeId with its namespace URI |
| node_alias | STRING | Configured alias or discovered browse name |
| parent_id, browse_name, node_class | STRING | Browse metadata for NODE |
| depth | INT | Depth from the browse root, starting at 1 |
| value_type, value_json | STRING | Built-in OPC UA type and JSON-encoded value |
| quality | STRING | GOOD, UNCERTAIN, or BAD |
| status_code | BIGINT | Unsigned OPC UA status code as a nonnegative long |
| source_time, server_time | STRING | UTC ISO 8601 timestamps with 100-nanosecond resolution |
| source_picoseconds, server_picoseconds | INT | Optional fractional fields in units of 10 picoseconds |
| diagnostic | STRING | Gap reason |

Values support Boolean, SByte, Byte, Int16, UInt16, Int32, UInt32, Int64, UInt64, Float, Double, String, DateTime, Guid, ByteString and arrays of these types. Integers keep their full precision. ByteString uses Base64. Non-finite floats use JSON strings. Bad values emit the JSON literal `null` and retain their status.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `endpoint` | string | — | OPC TCP endpoint advertised by the server, including its port and path. |
| `read_mode` | string | `POLLING` | POLLING, SUBSCRIBE, or BROWSE_NODES. |
| `node_fetch_mode` | string | `MANUAL` | MANUAL, FILE, or BROWSE node selection for value reads. |
| `nodes` | map | — | Manual aliases and OPC UA textual NodeIds. |
| `node_file` | string | — | JSON file containing alias to textual NodeId entries. |
| `browse_root` | string | `i=84` | Root textual NodeId for browsing. |
| `browse_page_size` | int | `100` | Maximum requested references per browse page. |
| `max_nodes` | int | `1024` | Maximum distinct nodes in one browse or explicit selection. |
| `max_depth` | int | `16` | Maximum depth below the browse root. |
| `interval_ms` | int | `1000` | Polling cadence or subscription publishing and sampling interval. |
| `refresh_interval_ms` | int | `3600000` | Refresh period for browse-selected value nodes. |
| `read_batch_size` | int | `100` | Maximum number of values or monitored items in one service call. |
| `queue_capacity` | int | `1024` | Maximum queued subscription samples per reader. |
| `max_records` | long | `0` | Maximum records for a bounded run. Zero means no row-count limit. |
| `request_timeout_ms` | int | `5000` | Deadline for one network operation or complete browse. |
| `session_timeout_ms` | int | `120000` | Requested OPC UA session timeout and reconnection budget. |
| `security_policy` | string | `Basic256Sha256` | None, Basic128Rsa15, Basic256, or Basic256Sha256. |
| `message_security_mode` | string | Policy-dependent | None, Sign, or SignAndEncrypt. |
| `allow_insecure` | boolean | `false` | Permit an explicitly chosen None endpoint. |
| `allow_legacy_security` | boolean | `false` | Permit a legacy Basic128Rsa15 or Basic256 endpoint. |
| `application_uri` | string | `urn:apache:seatunnel:opcua:client` | Client application URI, also present in the client certificate SAN. |
| `server_application_uri` | string | — | Expected server application URI for a secured endpoint. |
| `server_certificate_sha256` | string | — | SHA-256 fingerprint of the trusted server application certificate. |
| `client_key_store` | string | — | PKCS12 client key store on the worker. |
| `client_key_alias` | string | `client` | Client private key alias in the PKCS12 key store. |
| `password` | string | — | Password of the client PKCS12 key store and key. |
| `max_chunk_size` | int | `65536` | Maximum channel chunk size in bytes. |
| `max_chunk_count` | int | `32` | Maximum number of chunks in one channel message. |
| `max_message_size` | int | `2097152` | Maximum incoming OPC UA message bytes. |
| `max_array_length` | int | `65536` | Maximum decoded value array elements. |
| `max_string_bytes` | int | `65536` | Maximum individual normalized string or ByteString length. |

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  OpcUa {
    endpoint = "opc.tcp://opcua.example.org:4840/plant"
    read_mode = "POLLING"
    nodes = { temperature = "nsu=urn:factory:line1;s=temperature" }
    max_records = 1
    application_uri = "urn:example:seatunnel:opcua:client"
    server_application_uri = "urn:example:opcua:plant"
    server_certificate_sha256 = "<trusted-certificate-sha256>"
    client_key_store = "/etc/seatunnel/opcua-client.p12"
    password = ${OPCUA_KEYSTORE_PASSWORD}
  }
}
sink {
  Console {}
}
```

<ChangeLog />
