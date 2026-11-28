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

import ChangeLog from '../changelog/connector-coap.md';

# CoAP

The `CoAP` source listens on one UDP socket and accepts GET, POST, and PUT at the configured resource. Run it with source parallelism 1. Use `STREAMING` for continuous collection. Set `stop_after_messages` to a positive number for a bounded read.

The source uses plain UDP. Use a network appropriate for unauthenticated and unencrypted traffic. CoAP over DTLS (`coaps://`), TCP, and multicast are outside its protocol scope.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `bind_host` | String | `0.0.0.0` | Numeric listener address. |
| `port` | Integer | `5683` | UDP port, from 1 to 65535. |
| `resource_path` | String | `/sdc` | Absolute listener path with one to sixteen unreserved segments, each at most 64 characters. |
| `format` | Enum | `JSON` | Supports `JSON`, `TEXT`, `BINARY`, `CSV`, `AVRO`, `PROTOBUF`, `XML`, `LOG`, `DATAGRAM`, and `RECORD_JSON`. |
| `max_payload_bytes` | Integer | `65536` | Maximum assembled body bytes, from 16 to 1048576. |
| `queue_capacity` | Integer | `128` | Maximum admitted complete messages awaiting the reader, from 1 to 8192. |
| `max_peers` | Integer | `64` | Maximum tracked remote socket addresses, from 1 to 1024. |
| `max_concurrent_requests` | Integer | `4` | Number of protocol workers, from 1 to 32. |
| `blockwise_timeout_ms` | Integer | `30000` | Partial block transfer inactivity limit, from 500 to 120000 milliseconds. |
| `stop_after_messages` | Long | `0` | Continuous collection at zero. A positive value completes the source after that many messages. |
| `schema` | Map | — | Required for CSV, Avro, and Protobuf. Optional for JSON. |
| `csv_delimiter` | String | `,` | One printable non-quote delimiter for CSV. |
| `avro_writer_schema` | String | — | Optional schema for decoding raw Avro. Defaults to the schema derived from `schema`. |
| `protobuf_schema` | String | — | Protobuf schema content, required when `format = PROTOBUF`. |
| `protobuf_message_name` | String | — | Fully qualified message name, required when `format = PROTOBUF`. |
| `xml_record_path` | String | `/*` | XPath selecting the record elements. |
| `xml_namespaces` | Map | `{}` | Namespace URIs keyed by XPath prefix. |
| `xml_preserve_root` | Boolean | `false` | Keep the selected element name at the top of the output object. |
| `log_mode` | Enum | `COMMON_LOG_FORMAT` | Selects the log parser described below. |
| `log_pattern` | String | — | Custom regex, Grok expression, Apache layout, or Log4j layout. |
| `log_groups` | Map | `{}` | For REGEX mode, output field names mapped to capture indexes. |
| `grok_definitions` | Map | `{}` | Additional named Grok definitions using noncapturing regex groups. |
| `datagram_mode` | Enum | `RAW_DATA` | Selects `RAW_DATA`, `SYSLOG`, `NETFLOW`, or `COLLECTD`. |
| `datagram_separator` | String | — | Literal separator for RAW_DATA UTF-8 text, from 1 to 32 characters. When absent the source emits the complete byte array. |
| `netflow_max_templates` | Integer | `128` | Maximum cached NetFlow v9 templates, from 1 to 1024. |
| `netflow_template_timeout_ms` | Integer | `300000` | NetFlow v9 template lifetime, from 1000 to 3600000 milliseconds. |
| `collectd_username` | String | — | Username for authenticated collectd payloads. |
| `collectd_password` | String | — | Password for collectd signature verification and decryption. Configure it together with the username. |
| `collectd_types` | Map | `{}` | Comma-separated metric names by collectd type. |
| `collectd_convert_time` | Boolean | `true` | Normalize collectd timestamps and intervals to milliseconds. |
| `collectd_omit_interval` | Boolean | `false` | Omit the interval field from collectd output. |
| `record_header_field` | String | — | Name an output schema field to retain the RECORD_JSON wire header. |

## Message handling

| Format | Content-Format | Representation |
| --- | --- | --- |
| `JSON` | 50 | Valid UTF-8 JSON. A source without `schema` exposes the JSON text as `payload`. A source with `schema` expects an object and emits typed fields. A sink encodes the input row as an object. |
| `TEXT` | 0 | One strict UTF-8 string, including an empty string. |
| `BINARY` | 42 | One byte array, including zero bytes. |
| `CSV` | 0 | One delimited record, without a header. |
| `AVRO` | 42 | One raw Avro binary datum. The writer uses the Avro schema generated from the input row type. |
| `PROTOBUF` | 42 | One binary message using the supplied schema and message name. |
| `XML` | 41 | XPath-selected XML elements. Attributes use `@name`, repeated child elements become arrays, and mixed text uses `$text`. |
| `LOG` | 0 | Structured records decoded with `log_mode`. |
| `DATAGRAM` | 42 | An inner syslog, NetFlow, collectd, or raw datagram. |
| `RECORD_JSON` | 50 | One or more JSON records with a `header` and a recursively tagged `value`. |

The source uses the configured format when Content-Format is absent. A different explicit Content-Format receives 4.15. The configured resource returns 2.03 after a valid complete message enters the in-memory queue, 5.03 when admission is unavailable, and 4.00 for a malformed body. Californium handles CON/NON correlation, duplicate messages, and blockwise transfers. Incomplete or oversized blockwise bodies can receive the corresponding CoAP errors. A retransmission can reuse a cached response while the peer and message ID remain in the bounded deduplication window.

The listener limits admitted peers, incoming protocol tasks, outgoing datagrams and complete queued messages. It validates a combined 64 MiB buffer budget covering completed and partial bodies and protocol datagrams. Runtime memory also includes decoded rows, codec allocations, and SDK state. Datagrams can be lost before application admission when peer or protocol limits are reached. Large representations must use protocol blocks. Opaque formats such as Avro container files or application-specific envelopes can use `BINARY` with a separate decoder.

## Source formats and limits

Without a user schema, XML, LOG, and decoded DATAGRAM records are JSON strings in the `payload` column. RECORD_JSON retains the wire header and decodes field type tags and values. With a schema, these formats map fields to typed columns. Set `record_header_field` to also retain the RECORD_JSON header in a typed column. Each message can produce up to 1024 rows. The connector validates a message before admitting any of those rows. A NetFlow v9 template-only message can complete without an output row.

- XML uses local XPath and disables DTDs, external entities, and external resource access.
- LOG supports `COMMON_LOG_FORMAT`, `COMBINED_LOG_FORMAT`, `APACHE_ERROR_LOG_FORMAT`, `APACHE_CUSTOM_LOG_FORMAT`, `REGEX`, `GROK`, `LOG4J`, `CEF`, and `LEEF`. It reads one entry per line. The parser limits each line to 16384 characters and custom matching to one million character accesses per line. Custom Grok definitions can extend the built-in word, number, address, timestamp, and Apache access patterns.
- DATAGRAM accepts RFC 3164 and RFC 5424 syslog, NetFlow v5 and v9 template/options/data FlowSets, and collectd network packets. NetFlow cache keys include exporter IP and observation domain and expire after the selected lifetime. A v9 data FlowSet requires its exporter template. Configure both collectd credential fields to require signed or encrypted packets and authenticate their payloads. The template cache is local to the listener session.

## Checkpoint and delivery

Checkpoint state records how many complete messages were emitted and the listener configuration. Recovery rebinds the listener and resumes that count. A 2.03 receipt confirms admission into volatile memory. Sink persistence is a separate event. Incomplete transfers, queued messages, and protocol deduplication state are lost across restart. A restarted listener can accept a retransmitted message again. UDP provides no remote replay offset.

## Example

```hocon
env {
  job.mode = "STREAMING"
  parallelism = 1
}

source {
  CoAP {
    bind_host = "127.0.0.1"
    port = 5683
    resource_path = "/sensor/data"
    format = "JSON"
  }
}

sink {
  Console {}
}
```

<ChangeLog />
