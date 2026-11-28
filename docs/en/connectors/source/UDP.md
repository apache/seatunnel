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

import ChangeLog from '../changelog/connector-udp.md';

# UDP

The UDP source listens on configured worker ports and emits rows from independent datagrams. The end-to-end suite targets SeaTunnel Zeta.

Each port has one reader. Set the source parallelism from 1 to the number of ports. Ports are sorted and assigned round-robin. In a distributed job, send traffic to the worker hosting each reader. A second process or job cannot bind the same address and port. Workers use the local interface specified by `bind_host`.

## Source options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `ports` | List&lt;Integer&gt; | Yes | | One to 64 distinct UDP ports, each in 1–65,535. |
| `bind_host` | String | No | `0.0.0.0` | Local numeric IPv4 or IPv6 address. Hostnames and IPv6 scope IDs are excluded. |
| `format` | String | No | `TEXT` | `TEXT`, `BINARY`, `SYSLOG`, `COLLECTD`, or `NETFLOW_V5`, ignoring case. |
| `separator` | String | No | Empty | Literal delimiter of up to 64 UTF-8 bytes for `TEXT` and `BINARY`. |
| `max_datagram_bytes` | Integer | No | `65507` | Accepted payload size in bytes. Range 1–65,527. The IP stack may impose a lower limit. |
| `receive_buffer_bytes` | Integer | No | `262144` | Requested OS receive buffer for each socket, in bytes. Range 1,024–16,777,216. The OS may cap or adjust it. |
| `max_datagrams_per_poll` | Integer | No | `32` | Datagrams processed per reader per poll. Range 1–128. |
| `max_records_per_datagram` | Integer | No | `256` | Decoded rows allowed from one accepted datagram. Range 1–1,024. |
| `idle_timeout_ms` | Integer | No | `0` | Time without a datagram before a port closes and finishes. Range 0–86,400,000 ms. Zero listens continuously. BATCH requires a positive value. |
| `on_malformed` | String | No | `DROP` | `DROP` discards malformed, oversized or over-expanded datagrams. `FAIL` fails the job. |

## Output formats

All formats append `sender_host` (numeric IP string), `sender_port` (integer), `listen_port` (integer), and `record_index` (zero-based position within the datagram). The source obtains sender fields from the IP socket. They are unauthenticated.

| Format | Format-specific columns and behavior |
| --- | --- |
| `TEXT` | `payload` as a strict UTF-8 string. Preserves newlines and empty datagrams. With `separator`, splits only within a datagram and skips empty segments. |
| `BINARY` | `payload` as raw BYTES, including a zero-byte payload. If configured, the same byte delimiter rule applies. |
| `SYSLOG` | One RFC 5424 version 1 or RFC 3164 message per datagram. Outputs `facility`, `severity`, `version`, `event_time`, `hostname`, `app_name`, `process_id`, `message_id`, `structured_data`, and `message`. Both formats use strict UTF-8. Legacy timestamps retain their original yearless local text. Preserves the RFC 5424 structured-data text and maps NILVALUE header fields to null. [UDP transport specification](https://www.rfc-editor.org/rfc/rfc5426.html) |
| `COLLECTD` | Plaintext network-plugin metrics. Outputs `hostname`, `time_ms`, `interval_ms`, `plugin`, `plugin_instance`, `metric_type`, `type_instance`, `value_index`, `value_type`, `integer_value`, and `gauge_value`. Unsigned counter/absolute values fit DECIMAL(20,0). Signed derive values also use the integer column. Gauges use DOUBLE. Signed/encrypted packets, notifications and types database aliases are unsupported. [collectd wire format](https://github.com/collectd/collectd/wiki/Binary-protocol) |
| `NETFLOW_V5` | One row per flow, with header columns `uptime_ms`, `unix_seconds`, `unix_nanoseconds`, `flow_sequence`, `engine_type`, `engine_id`, `sampling_mode`, and `sampling_interval`. Address and counter columns `source_ip`, `destination_ip`, `next_hop`, `input_interface`, `output_interface`, `packets`, `octets`, `first_ms`, and `last_ms`. Additional columns are `source_port`, `destination_port`, `tcp_flags`, `protocol`, `tos`, `source_as`, `destination_as`, `source_mask`, and `destination_mask`. NetFlow v9 and IPFIX are unsupported. [Cisco wire format](https://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/5-0-3/user/guide/format.html) |

## Delivery and shutdown

UDP provides best-effort delivery without acknowledgements. Packets may arrive out of order, be duplicated upstream, or be dropped. When downstream blocks, incoming packets wait in the OS receive buffer. The OS drops packets if it runs out of space. Source counters `udpMalformedDatagrams` and `udpOversizedDatagrams` report locally rejected packets. They cannot count packets the OS dropped before delivery. A discarded datagram emits no partial records.

A checkpoint saves port ownership and whether each listener has finished. It carries neither incoming packets nor replay offsets. After restore, live ports bind again with a fresh inactivity interval. Finished ports stay closed. Keep the configured port list fixed when restoring. The source emits a datagram at most once per reception and cannot suppress duplicates presented by the sender or network.

## Example

```hocon
env {
  job.mode = "STREAMING"
  parallelism = 1
}

source {
  UDP {
    bind_host = "0.0.0.0"
    ports = [5514]
    format = "SYSLOG"
  }
}

sink {
  Console {}
}
```

<ChangeLog />
