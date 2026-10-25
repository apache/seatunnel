import ChangeLog from '../changelog/connector-syslog.md';

# Syslog

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

> RFC 5424 sink over TLS, with RFC 5425 octet-counted framing.

## Description

Sends INSERT rows to a TLS syslog receiver. This sink does not listen for syslog messages.
RFC 3164, plaintext TCP, UDP, RELP, transactions and automatic reconnect/retry are not supported.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

A receiver supporting RFC 5424 over TLS with RFC 5425 framing, for example a syslog-ng
`syslog(transport("tls"))` source. A legacy newline-framed TCP listener is not compatible.
No additional client library is required; the connector uses JDK TLS.
Set the message-size cap at or below the receiver's limit; not every receiver accepts 8192 bytes.

## Sink Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| host | String | Yes | - | Receiver DNS name or IP address, without scheme or port. Must match the certificate. |
| port | Int | No | 6514 | Receiver port, 1-65535. |
| connect_timeout_ms | Int | No | 10000 | Positive TCP connect timeout. JVM/platform DNS resolution is outside this deadline. |
| write_timeout_ms | Int | No | 10000 | Positive deadline for each TLS handshake, write/flush, and close operation. |
| max_message_bytes | Int | No | 8192 | Maximum encoded message size, 1-1048576 bytes, including header, structured data and UTF-8 BOM but excluding length prefix. Oversize messages fail; they are not truncated. |
| tls.ca_cert_path | String | No | - | Worker-local PEM CA certificate bundle. Absent means JVM default trust anchors. |
| tls.key_store.path | String | No | - | Optional worker-local client key store for mutual TLS. |
| password | String | With key store | - | Password for both client key store and private key. |
| tls.key_store.type | String | No | PKCS12 | Client store type: `PKCS12` or `JKS`. |
| common-options | | No | - | [Common Sink Options](../common-options/sink-common-options.md), including `plugin_input`. |

## Input Schema

Column names are fixed; rename columns with a transform when needed. Extra columns are ignored.
If an optional column is present, it must have the listed type. Schema validation does not access the network or TLS files.

| Column | Type | Absent or null | Contract |
|--------|------|----------------|----------|
| message | STRING | Null omits MSG; column is required | UTF-8 text. Empty text is distinct from null. Non-null MSG includes the UTF-8 BOM. |
| facility | INT | 1 (user-level) | 0-23. PRI is facility * 8 + severity. |
| severity | INT | 6 (informational) | 0-7. |
| timestamp | STRING | `-` | RFC 5424 timestamp with uppercase T/Z or numeric offset; at most six fractional digits. Invalid dates, leap seconds and local timestamps are rejected. No timestamp is invented. |
| hostname | STRING | `-` | 1-255 printable US-ASCII characters, no spaces. |
| app_name | STRING | `-` | 1-48 printable US-ASCII characters, no spaces. |
| proc_id | STRING | `-` | 1-128 printable US-ASCII characters, no spaces. |
| msg_id | STRING | `-` | 1-32 printable US-ASCII characters, no spaces. |
| structured_data | MAP&lt;STRING, MAP&lt;STRING, STRING&gt;&gt; | `-` (also for empty map) | SD-ID to parameter map. No raw structured-data strings. |

SD-ID and parameter names contain 1-32 printable ASCII characters, excluding space, `=`, `]` and `"`.
Parameter values must be non-null strings; empty parameter maps are allowed.
Quotes, backslashes and closing brackets in parameter values are escaped.
Use IANA-registered SD-IDs or your own private-enterprise-number suffix; `example@32473` is for documentation only.
Malformed Unicode is rejected. Newlines and other control characters in MSG and parameter values are preserved.
Octet framing keeps them inside a single message, but receiver storage/display policies may alter them.
Header control characters and spaces are rejected rather than escaped.
RFC 3164 source timestamps such as `Oct 11 22:14:15` require an explicit conversion before use here.

## Delivery and Resource Limits

- Each writer opens one TLS connection and sends one message synchronously, without an asynchronous message queue.
  Only one write is in flight per writer. The byte cap bounds each frame and intermediate encoding buffers.
- A successful write or checkpoint flush means only that the local TLS/socket operation completed.
  Syslog has no application-level acknowledgment of receiver parsing, durable storage, or downstream delivery.
  This sink promises neither at-least-once nor exactly-once delivery. Failures/recovery can cause loss or duplicates.
- Partial writes, timeouts and detected disconnects permanently fail that writer. No message is silently retried.
  A new engine task may replay input on recovery; the receiver can then observe duplicates.
- Both prepare-commit hooks, snapshot and close flush/check the connection and propagate transport failures.
  There is no transactional commit or recoverable writer state.
- A watchdog closes the underlying TCP socket to interrupt stalled TLS output. Socket read timeout alone is not a write timeout.
  Concurrent close aborts an active write. Interrupting the writing thread may take up to the operation deadline to unblock it.
  DNS resolution still follows JVM/platform resolver limits; configure those separately.
- Keep operation deadlines below the job checkpoint timeout. Parallel writers have separate connections and no global ordering.
  A returned local write cannot detect every peer failure, including a receiver that discards data after accepting it.

## TLS and Security

TLS is mandatory. Only TLS 1.2 and TLS 1.3 supported by the worker JDK are enabled.
The receiver chain is verified with the configured CA bundle or JVM trust anchors and the configured host
is checked using JSSE HTTPS endpoint identification. There is no trust-all or hostname-validation bypass.
Do not change JVM-global trust settings to configure this connector.

Deploy trust/client key files to every worker. Restrict access to client private keys, and supply
`password` through configuration substitution or the deployment's secret management.
Never commit real passwords. Mutual TLS requires both the key-store path and password; the receiver must trust that client certificate.
TLS authenticates the transport peer, not the `hostname`/application identity supplied in a row.

## Example

Configure the receiver to accept RFC 5424 over TLS with octet counting, then replace the endpoint and CA path below.
The runnable example is `seatunnel-connectors-v2/connector-syslog/examples/fake_to_syslog.conf`.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  FakeSource {
    row.num = 1
    schema.fields {
      facility = int
      severity = int
      timestamp = string
      hostname = string
      app_name = string
      proc_id = string
      msg_id = string
      structured_data = "map<string, map<string, string>>"
      message = string
    }
    rows = [{
      kind = INSERT
      fields = [1, 6, "2026-01-02T03:04:05.123Z", "origin", "seatunnel", "-", "ID47",
        {"example@32473": {"component": "pipeline"}}, "job completed"]
    }]
  }
}
sink {
  Syslog {
    host = "syslog.example.org"
    port = 6514
    tls.ca_cert_path = "/etc/seatunnel/syslog-ca.pem"
    connect_timeout_ms = 10000
    write_timeout_ms = 10000
    max_message_bytes = 8192
  }
}
```

Optional client authentication:

```hocon
tls.key_store.path = "/etc/seatunnel/syslog-client.p12"
tls.key_store.type = "PKCS12"
password = ${SYSLOG_KEY_STORE_PASSWORD}
```

<ChangeLog />
